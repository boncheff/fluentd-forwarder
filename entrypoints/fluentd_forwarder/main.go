package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"reflect"

	fluentd_forwarder "github.com/boncheff/fluentd-forwarder/forwarder"
	metro "github.com/boncheff/fluentd-forwarder/metro"
	"github.com/ugorji/go/codec"
)

const (
	LISTEN_ON = "127.0.0.1:24224"
)

func main() {
	producer := metro.NewMetroProducer()

	_codec := codec.MsgpackHandle{}
	_codec.MapType = reflect.TypeOf(map[string]interface{}(nil))
	_codec.RawToString = false
	addr, err := net.ResolveTCPAddr("tcp", LISTEN_ON)
	if err != nil {
		log.Printf("Error resolving TCP address %s:  %+v", LISTEN_ON, err.Error())
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

	log.Println("Shutting down...")
}
