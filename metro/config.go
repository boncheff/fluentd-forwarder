package metro

import (
	log "github.com/Sirupsen/logrus"
	"github.com/kelseyhightower/envconfig"
)

// Struct to hold Metro Kafka config - populated from environment
type MetroConfig struct {
	KafkaHosts []string `envconfig:"kafka_hosts" default:"172.17.42.1:9092"`
	RegionTag  string   `envconfig:"region_tag" default:"eu"`
	KafkaTopic string   `default:".messages"`
	Component  string   `default:"fluentd-forwarder"`
}

var conf MetroConfig

// At module init time, populate the config
func init() {
	err := envconfig.Process("bespin", &conf)
	if err != nil {
		log.Fatalf("Failed to load Vader Config: %s", err.Error())
	}
	conf.KafkaTopic = conf.RegionTag + conf.KafkaTopic

	log.Debugf("CONFIG: %+v", conf)
}
