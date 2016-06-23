package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	fluentd_forwarder "github.com/boncheff/fluentd-forwarder/forwarder"
	metro "github.com/boncheff/fluentd-forwarder/metro"
	strftime "github.com/jehiah/go-strftime"
	ioextras "github.com/moriyoshi/go-ioextras"
	logging "github.com/op/go-logging"
	"github.com/ugorji/go/codec"
	gcfg "gopkg.in/gcfg.v1"
)

type FluentdForwarderParams struct {
	RetryInterval       time.Duration
	ConnectionTimeout   time.Duration
	WriteTimeout        time.Duration
	FlushInterval       time.Duration
	Parallelism         int
	JournalGroupPath    string
	MaxJournalChunkSize int64
	ListenOn            string
	OutputType          string
	ForwardTo           string
	LogLevel            logging.Level
	LogFile             string
	DatabaseName        string
	TableName           string
	ApiKey              string
	Ssl                 bool
	SslCACertBundleFile string
	CPUProfileFile      string
	Metadata            string
}

type PortWorker interface {
	fluentd_forwarder.Port
	fluentd_forwarder.Worker
}

var progName = os.Args[0]
var progVersion string

func MustParseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		panic(err)
	}
	return d
}

func Error(fmtStr string, args ...interface{}) {
	fmt.Fprint(os.Stderr, progName, ": ")
	fmt.Fprintf(os.Stderr, fmtStr, args...)
	fmt.Fprint(os.Stderr, "\n")
}

type LogLevelValue logging.Level

func (v *LogLevelValue) String() string {
	return logging.Level(*v).String()
}

func (v *LogLevelValue) Set(s string) error {
	_v, err := logging.LogLevel(s)
	*v = LogLevelValue(_v)
	return err
}

func updateFlagsByConfig(configFile string, flagSet *flag.FlagSet) error {
	config := struct {
		Fluentd_Forwarder struct {
			Retry_interval     string `retry-interval`
			Conn_timeout       string `conn-timeout`
			Write_timeout      string `write-timeout`
			Flush_interval     string `flush-interval`
			Listen_on          string `listen-on`
			To                 string `to`
			Buffer_path        string `buffer-path`
			Buffer_chunk_limit string `buffer-chunk-limit`
			Log_level          string `log-level`
			Ca_certs           string `ca-certs`
			Cpuprofile         string `cpuprofile`
			Log_file           string `log-file`
		}
	}{}
	err := gcfg.ReadFileInto(&config, configFile)
	if err != nil {
		return err
	}
	r := reflect.ValueOf(config.Fluentd_Forwarder)
	rt := r.Type()
	for i, l := 0, rt.NumField(); i < l; i += 1 {
		f := rt.Field(i)
		fv := r.Field(i)
		v := fv.String()
		if v != "" {
			err := flagSet.Set(string(f.Tag), v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ParseArgs() *FluentdForwarderParams {
	configFile := ""
	retryInterval := (time.Duration)(0)
	connectionTimeout := (time.Duration)(0)
	writeTimeout := (time.Duration)(0)
	flushInterval := (time.Duration)(0)
	parallelism := 0
	listenOn := ""
	forwardTo := ""
	journalGroupPath := ""
	maxJournalChunkSize := int64(16777216)
	logLevel := LogLevelValue(logging.INFO)
	sslCACertBundleFile := ""
	cpuProfileFile := ""
	logFile := ""
	metadata := ""

	flagSet := flag.NewFlagSet(progName, flag.ExitOnError)

	flagSet.StringVar(&configFile, "config", "", "configuration file")
	flagSet.DurationVar(&retryInterval, "retry-interval", 0, "retry interval in which connection is tried against the remote agent")
	flagSet.DurationVar(&connectionTimeout, "conn-timeout", MustParseDuration("10s"), "connection timeout")
	flagSet.DurationVar(&writeTimeout, "write-timeout", MustParseDuration("10s"), "write timeout on wire")
	flagSet.DurationVar(&flushInterval, "flush-interval", MustParseDuration("5s"), "flush interval in which the events are forwareded to the remote agent")
	flagSet.IntVar(&parallelism, "parallelism", 1, "Number of chunks to submit at once (for td output)")
	flagSet.StringVar(&listenOn, "listen-on", "127.0.0.1:24224", "interface address and port on which the forwarder listens")
	flagSet.StringVar(&forwardTo, "to", "fluent://127.0.0.1:24225", "host and port to which the events are forwarded")
	flagSet.StringVar(&journalGroupPath, "buffer-path", "*", "directory / path on which buffer files are created. * may be used within the path to indicate the prefix or suffix like var/pre*suf")
	flagSet.Int64Var(&maxJournalChunkSize, "buffer-chunk-limit", 16777216, "Maximum size of a buffer chunk")
	flagSet.Var(&logLevel, "log-level", "log level (defaults to INFO)")
	flagSet.StringVar(&sslCACertBundleFile, "ca-certs", "", "path to SSL CA certificate bundle file")
	flagSet.StringVar(&cpuProfileFile, "cpuprofile", "", "write CPU profile to file")
	flagSet.StringVar(&logFile, "log-file", "", "path of the log file. log will be written to stderr if unspecified")
	flagSet.StringVar(&metadata, "metadata", "", "set addtional data into record")
	flagSet.Parse(os.Args[1:])

	if configFile != "" {
		err := updateFlagsByConfig(configFile, flagSet)
		if err != nil {
			Error("%s", err.Error())
			os.Exit(1)
		}
	}

	ssl := false
	outputType := ""
	databaseName := "*"
	tableName := "*"
	apiKey := ""

	if strings.Contains(forwardTo, "//") {
		u, err := url.Parse(forwardTo)
		if err != nil {
			Error("%s", err.Error())
			os.Exit(1)
		}
		switch u.Scheme {
		case "fluent", "fluentd":
			outputType = "fluent"
			forwardTo = u.Host
		case "td+http", "td+https":
			outputType = "td"
			forwardTo = u.Host
			if u.User != nil {
				apiKey = u.User.Username()
			}
			if u.Scheme == "td+https" {
				ssl = true
			}
			p := strings.Split(u.Path, "/")
			if len(p) > 1 {
				databaseName = p[1]
			}
			if len(p) > 2 {
				tableName = p[2]
			}
		}
	} else {
		outputType = "fluent"
	}
	if outputType == "" {
		Error("Invalid output specifier")
		os.Exit(1)
	} else if outputType == "fluent" {
		if !strings.ContainsRune(forwardTo, ':') {
			forwardTo += ":24224"
		}
	}
	return &FluentdForwarderParams{
		RetryInterval:       retryInterval,
		ConnectionTimeout:   connectionTimeout,
		WriteTimeout:        writeTimeout,
		FlushInterval:       flushInterval,
		Parallelism:         parallelism,
		ListenOn:            listenOn,
		OutputType:          outputType,
		ForwardTo:           forwardTo,
		Ssl:                 ssl,
		DatabaseName:        databaseName,
		TableName:           tableName,
		ApiKey:              apiKey,
		JournalGroupPath:    journalGroupPath,
		MaxJournalChunkSize: maxJournalChunkSize,
		LogLevel:            logging.Level(logLevel),
		LogFile:             logFile,
		SslCACertBundleFile: sslCACertBundleFile,
		CPUProfileFile:      cpuProfileFile,
		Metadata:            metadata,
	}
}

func ValidateParams(params *FluentdForwarderParams) bool {
	if params.RetryInterval < 0 {
		Error("Retry interval may not be negative")
		return false
	}
	if params.RetryInterval > 0 && params.RetryInterval < 100000000 {
		Error("Retry interval must be greater than or equal to 100ms")
		return false
	}
	if params.FlushInterval < 100000000 {
		Error("Flush interval must be greater than or equal to 100ms")
		return false
	}
	if params.FlushInterval < 100000000 {
		Error("Flush interval must be greater than or equal to 100ms")
		return false
	}
	switch params.OutputType {
	case "fluent":
		if params.RetryInterval == 0 {
			params.RetryInterval = MustParseDuration("5s")
		}
		if params.RetryInterval > params.FlushInterval {
			Error("Retry interval may not be greater than flush interval")
			return false
		}
	case "td":
		if params.RetryInterval != 0 {
			Error("Retry interval will be ignored")
			return false
		}
	}
	return true
}

func main() {
	params := ParseArgs()
	if !ValidateParams(params) {
		os.Exit(1)
	}
	logWriter := (io.Writer)(nil)
	if params.LogFile != "" {
		logWriter = ioextras.NewStaticRotatingWriter(
			func(_ interface{}) (string, error) {
				path := strftime.Format(params.LogFile, time.Now())
				return path, nil
			},
			func(path string, _ interface{}) (io.Writer, error) {
				dir, _ := filepath.Split(path)
				err := os.MkdirAll(dir, os.FileMode(0777))
				if err != nil {
					return nil, err
				}
				return os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.FileMode(0666))
			},
			nil,
		)
	} else {
		logWriter = os.Stderr
	}
	logBackend := logging.NewLogBackend(logWriter, "[fluentd-forwarder] ", log.Ldate|log.Ltime|log.Lmicroseconds)
	logging.SetBackend(logBackend)
	logger := logging.MustGetLogger("fluentd-forwarder")
	logging.SetLevel(params.LogLevel, "fluentd-forwarder")
	if progVersion != "" {
		logger.Infof("Version %s starting...", progVersion)
	}

	//-----------------------------

	producer := metro.NewLogHandler()

	//-----------------------------

	_codec := codec.MsgpackHandle{}
	_codec.MapType = reflect.TypeOf(map[string]interface{}(nil))
	_codec.RawToString = false
	addr, err := net.ResolveTCPAddr("tcp", params.ListenOn)
	if err != nil {
		log.Printf("Error resolving TCP address %s:  %+v", params.ListenOn, err.Error())
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Printf("Error listening on %s:  %+v", addr, err.Error())
		fmt.Println(err.Error())
	}
	acceptChan := make(chan *net.TCPConn)

	fluentd_forwarder.Start(producer, *listener, acceptChan, _codec)

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, os.Interrupt, os.Kill)
	<-wait

	logger.Notice("Shutting down...")
}
