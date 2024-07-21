package queue

import (
	"encoding/json"
	"github.com/IBM/sarama"
)

type MessageSerializer[Input any, Output any] interface {
	Serialize(input Input) (Output, error)
}

type JSONSerializer[Output any] struct {
}

func (s *JSONSerializer[Output]) Serialize(input []byte) (o Output, err error) {
	err = json.Unmarshal(input, &o)
	if err != nil {
		return o, err
	}

	return o, nil
}

func NewJSONSerializer[Output any]() *JSONSerializer[Output] {
	return &JSONSerializer[Output]{}
}

type KafkaJSONSerializer[Output any] struct {
	JSONSerializer[Output]
}

func (s *KafkaJSONSerializer[Output]) Serialize(input *sarama.ConsumerMessage) (Output, error) {
	return s.JSONSerializer.Serialize(input.Value)
}

func NewKafkaJSONSerializer[Output any]() *KafkaJSONSerializer[Output] {
	return &KafkaJSONSerializer[Output]{}
}
