package metro

import (
	"encoding/json"

	"github.com/Shopify/sarama"
	"github.com/Sirupsen/logrus"
)

const KAFKA_TOPIC = "eu.messages"

// Modified from github.com/goibibo/KafkaLogrus
type LogHandler struct {
	service  string
	producer sarama.AsyncProducer
	topic    string
}

func NewLogHandler() *LogHandler {

	// Init config
	config := sarama.NewConfig()

	producer, err := sarama.NewAsyncProducer(conf.KafkaHosts, config)
	if err != nil {
		logrus.Fatalf("Failed to start log producer: %s", err)
	}

	handler := LogHandler{
		service:  conf.Component,
		producer: producer,
		topic:    KAFKA_TOPIC,
	}

	return &handler
}

func (l *LogHandler) Fire(entry *logrus.Entry) error {

	payload := Log{
		Message:  entry.Message,
		Service:  l.service,
		Severity: entry.Level.String(),
		//	SourceHost
		//	CustomerId
		//	UserId        string            `json:"user_id"`
		//	TransactionId
		//	ResourceIds
		//	Debug
		//	Extra
	}
	message := LogMessage{
		Type:    "message",
		Payload: payload,
		//		Resource: metro.Resource{
		//			Id:       "3e08f2b9-d9ec-4381-b0ad-50e268ad3979",
		//			Location: "eu",
		//		},
		//		ProcessingLog: map[string]string{
		//			"metrics.customer_api": "2016-04-28T15:02:01.56888Z",
		//		},
		//		Time: "2016-04-28T15:04:01.56888Z",
		//		CustomerId: "00000000-0000-0000-0000-000000000001",
		//		Component:  "vader.aggregator",
		//		Location:   "eu",
		//		MessageId:  "67e2b225-e7da-49b0-a428-7dd6fdeef6fb",
	}

	json, err := json.Marshal(message)
	if err != nil {
		logrus.Printf("Error marshalling metric %s - %s", message, err)
	}
	l.producer.Input() <- &sarama.ProducerMessage{Topic: l.topic, Value: sarama.StringEncoder(json)}
	select {
	case err := <-l.producer.Errors():
		return err
	default:
		//Nothing to do here
	}
	return nil
}

func (*LogHandler) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}
