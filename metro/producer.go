package metro

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

// The Producer interface is going to be passed to all methods that use
// producer so that mock producers can be passed instead of real ones
type Producer interface {
	Close() error
	Produce(message MetricMessage)
}

type MetroProducer struct {
	producer sarama.AsyncProducer
	topic    string
}

const verbose = false

/* NewMetroProducer creates an AsyncProducer publishes Kafka messages using a
non-blocking API
*/
func NewMetroProducer() *MetroProducer {
	log.Info("Starting Metro Producer")

	// Init config
	config := sarama.NewConfig()
	if verbose {
		sarama.Logger = log.StandardLogger()
	} else {
		config.Producer.Return.Errors = true
	}

	producer, err := sarama.NewAsyncProducer(conf.KafkaHosts, config)
	if err != nil {
		log.Fatalf("Failed to start producer: %s", err)
	}

	return &MetroProducer{
		producer: producer,
		topic:    conf.KafkaTopic,
	}
}

// Closes the consumer
func (p *MetroProducer) Close() (err error) {
	log.Info("Closing Producer")
	return p.producer.Close()
}

//Produces a message
func (p *MetroProducer) Produce(item FluentRecordSet) error {
	var data string
	var dockerTag = item.Tag

	for _, rec := range item.Records {
		data = dockerTag
		for key, value := range rec.Data {
			d := string(value.([]uint8))
			data = data + fmt.Sprintf(" %s: %s", key, d)
		}
	}

	payload := Log{
		Message:     data,
		Service:     conf.Component,
		Severity:    "INFO",
		ResourceIds: []string{},
	}
	message := LogMessage{
		Type:    "message",
		Payload: payload,
		Resource: Resource{
			Id:       dockerTag,
			Location: "eu",
		},
		ProcessingLog: map[string]string{
			conf.Component: time.Now().UTC().Format("2006-01-02T15:04:05.999999Z"),
		},
		Time:      time.Now().UTC().Format("2006-01-02T15:04:05.999999Z"),
		Component: conf.Component,
	}

	json, err := json.Marshal(message)
	if err != nil {
		log.Printf("Error marshalling metric %s - %s", message, err)
	}
	p.producer.Input() <- &sarama.ProducerMessage{Topic: p.topic, Value: sarama.StringEncoder(json)}
	select {
	case err := <-p.producer.Errors():
		return err
	default:
		// Nothing to see here - move along...
	}
	return nil
}
