package kafkaqueue

import (
	"github.com/IBM/sarama"
	kafkaqueue "github.com/violetpay-org/event-queue/internal/queue/kafka"
	"github.com/violetpay-org/event-queue/queue"
)

func NewConsumeOperator[Msg any](serializer queue.MessageSerializer[*sarama.ConsumerMessage, Msg], callback queue.Callback[Msg], brokers []string, topic string, groupId string, config *sarama.Config) *kafkaqueue.ConsumeOperator[Msg] {
	return kafkaqueue.NewConsumeOperator(serializer, callback, brokers, topic, groupId, config)
}

func NewAckConsumeOperator[Msg any](serializer queue.MessageSerializer[*sarama.ConsumerMessage, Msg], ackCallback queue.AckCallback[Msg], callback queue.Callback[Msg], brokers []string, topic string, groupId string, config *sarama.Config) *kafkaqueue.AckConsumeOperator[Msg] {
	return kafkaqueue.NewAckConsumeOperator(serializer, ackCallback, callback, brokers, topic, groupId, config)
}

func NewProduceOperatorCtor(brokers []string, configProvider func() *sarama.Config) *kafkaqueue.BytesProduceOperatorCtor {
	return kafkaqueue.NewBytesProduceOperatorCtor(brokers, configProvider)
}

func NewBytesProduceOperator(brokers []string, topic string, configProvider func() *sarama.Config) *kafkaqueue.BytesProduceOperator {
	return kafkaqueue.NewBytesProduceOperator(brokers, topic, configProvider)
}
