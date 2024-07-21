package kafkaqueue

import (
	"github.com/IBM/sarama"
	"github.com/violetpay-org/event-queue/queue"
	"testing"
)

var brokers = []string{"localhost:9093"}

func mapOperatorProvider(queueName string, serializer queue.MessageSerializer[*sarama.ConsumerMessage, map[string]string], callback queue.Callback[map[string]string]) queue.ConsumeOperator[*sarama.ConsumerMessage, map[string]string] {
	return NewConsumeOperator(serializer, callback, brokers, queueName, "test-group-id", sarama.NewConfig())
}

func rawMsgProvider() *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{}
}

func TestConsumeOperator(t *testing.T) {
	t.Run("map[string]string", func(t *testing.T) {
		queue.TestSuiteConsumeOperator[*sarama.ConsumerMessage, map[string]string](
			t,
			mapOperatorProvider,
			rawMsgProvider,
		)
	})
}
