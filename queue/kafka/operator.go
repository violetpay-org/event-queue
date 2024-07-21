package kafkaqueue

import (
	"github.com/IBM/sarama"
	kafkaqueue "github.com/violetpay-org/new-queue-manager/internal/queue/kafka"
	"github.com/violetpay-org/new-queue-manager/queue"
)

func NewConsumeOperator[Msg any](serializer queue.MessageSerializer[*sarama.ConsumerMessage, Msg], callback queue.Callback[Msg], brokers []string, topic string, groupId string, config *sarama.Config) *kafkaqueue.ConsumeOperator[Msg] {
	return kafkaqueue.NewConsumeOperator(serializer, callback, brokers, topic, groupId, config)
}

func NewProduceOperator[Msg any](brokers []string, configProvider func() *sarama.Config) *kafkaqueue.BeforeBytesProduceOperator {
	return kafkaqueue.NewBytesProduceOperatorCtor(brokers, configProvider)
}
