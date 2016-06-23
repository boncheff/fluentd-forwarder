package metro

type MetricMessage struct {
	Resource      Resource          `json:"resource"`
	ProcessingLog map[string]string `json:"processing_log"`
	Time          string            `json:"time"`
	CustomerId    string            `json:"customer_id"`
	Type          string            `json:"type"`
	Payload       Metric            `json:"payload"`
	Issuer        Issuer            `json:"issuer"`
	MessageTags   []string          `json:"message_tags"`
}

type LogMessage struct {
	Resource      Resource          `json:"resource"`
	ProcessingLog map[string]string `json:"processing_log"`
	Time          string            `json:"time"`
	CustomerId    string            `json:"customer_id"`
	Type          string            `json:"type"`
	Payload       Log               `json:"payload"`
	Component     string            `json:"component"`
	Location      string            `json:"location"`
	MessageId     string            `json:"message_id"`
}

type Issuer struct {
	Component string `json:"component"`
	Location  string `json:"location"`
	MessageId string `json:"message_id"`
}

type Resource struct {
	Id       string `json:"id"`
	Location string `json:"location"`
}

type Metric struct {
	Metric           string  `json:"metric"`
	MonitoringSystem string  `json:"monitoring_system"`
	Value            float64 `json:"value"`
	Unit             string  `json:"unit"`
	TTL              int64   `json:"ttl"`
}

type Log struct {
	Message       string            `json:"message"`
	Service       string            `json:"service"`
	Severity      string            `json:"severity"`
	SourceHost    string            `json:"source_host"`
	CustomerId    string            `json:"customer_id"`
	UserId        string            `json:"user_id"`
	TransactionId string            `json:"transaction_id"`
	ResourceIds   []string          `json:"resource_ids"`
	Debug         map[string]string `json:"debug"`
	Extra         map[string]string `json:"extra"`
}

type FluentRecordSet struct {
	Tag     string
	Records []TinyFluentRecord
}

type TinyFluentRecord struct {
	Timestamp uint64
	Data      map[string]interface{}
}
