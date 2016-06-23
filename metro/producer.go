package metro

import (
	"encoding/json"
	"fmt"

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

	for _, rec := range item.Records {
		for key, value := range rec.Data {
			d := string(value.([]uint8))
			data = fmt.Sprintf("TAG: %s,   %s=>%s \n", item.Tag, key, d)
			// fmt.Printf("TAG: %s,  TIMESTAMP: %s, %s=>%s \n", item.Tag, rec.Timestamp, key, d)
		}
	}

	payload := Log{
		Message:  data,
		Service:  "fluentd-forwarder",
		Severity: "INFO",
		//  SourceHost
		//  CustomerId
		//  UserId        string            `json:"user_id"`
		//  TransactionId
		//  ResourceIds
		//  Debug
		//  Extra
	}
	message := LogMessage{
		Type:    "message",
		Payload: payload,
		//      Resource: metro.Resource{
		//          Id:       "3e08f2b9-d9ec-4381-b0ad-50e268ad3979",
		//          Location: "eu",
		//      },
		//      ProcessingLog: map[string]string{
		//          "metrics.customer_api": "2016-04-28T15:02:01.56888Z",
		//      },
		//      Time: "2016-04-28T15:04:01.56888Z",
		//      CustomerId: "00000000-0000-0000-0000-000000000001",
		//      Component:  "vader.aggregator",
		//      Location:   "eu",
		//      MessageId:  "67e2b225-e7da-49b0-a428-7dd6fdeef6fb",
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
		//Nothing to do here
	}
	return nil
}
