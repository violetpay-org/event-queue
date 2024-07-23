package kafkaqueue

import (
	"github.com/IBM/sarama"
	"github.com/violetpay-org/event-queue/queue"
	"testing"
)

var brokers = []string{"kafka.vp-datacenter-1.violetpay.net:9092", "kafka.vp-datacenter-1.violetpay.net:9093", "kafka.vp-datacenter-1.violetpay.net:9094"}

func mapConsumeOperatorProvider(queueName string, serializer queue.MessageSerializer[*sarama.ConsumerMessage, map[string]string], callback queue.Callback[map[string]string]) queue.ConsumeOperator[*sarama.ConsumerMessage, map[string]string] {
	return NewConsumeOperator(serializer, callback, brokers, queueName, "test-group-id", sarama.NewConfig())
}

func mapAckConsumeOperatorProvider(queueName string, serializer queue.MessageSerializer[*sarama.ConsumerMessage, map[string]string], callback queue.Callback[map[string]string]) queue.ConsumeOperator[*sarama.ConsumerMessage, map[string]string] {
	f := func(_ map[string]string) bool {
		return true
	}
	return NewAckConsumeOperator(serializer, f, callback, brokers, queueName, "test-group-id", sarama.NewConfig())
}

func consumeMessageProvider() *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{}
}

func TestConsumeOperator(t *testing.T) {
	t.Run("map[string]string", func(t *testing.T) {
		queue.TestSuiteConsumeOperator[*sarama.ConsumerMessage, map[string]string](
			t,
			mapConsumeOperatorProvider,
			consumeMessageProvider,
		)
	})

}

func TestAckConsumeOperator(t *testing.T) {
	t.Run("map[string]string", func(t *testing.T) {
		queue.TestSuiteConsumeOperator[*sarama.ConsumerMessage, map[string]string](
			t,
			mapAckConsumeOperatorProvider,
			consumeMessageProvider,
		)
	})
}

func bytesProduceOperatorProvider(queueName string) queue.ProduceOperator[[]byte] {
	return NewBytesProduceOperator(brokers, queueName, func() *sarama.Config {
		return sarama.NewConfig()
	})
}

func produceBytesMessageProvider() []byte {
	return []byte("test")
}

func TestBytesProduceOperator(t *testing.T) {
	queue.TestSuiteProduceOperator(
		t,
		bytesProduceOperatorProvider,
		produceBytesMessageProvider,
	)
}
