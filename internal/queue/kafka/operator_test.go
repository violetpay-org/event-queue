package kafkaqueue

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/violetpay-org/event-queue/queue"
)

var brokers = []string{"kafka.vp-datacenter-1.violetpay.net:9092", "kafka.vp-datacenter-1.violetpay.net:9093", "kafka.vp-datacenter-1.violetpay.net:9094"}

func mapOperatorProvider(
	queueName string,
	serializer queue.MessageSerializer[*sarama.ConsumerMessage, map[string]string],
	callback queue.Callback[map[string]string],
) queue.ConsumeOperator[*sarama.ConsumerMessage, map[string]string] {
	return NewConsumeOperator(
		serializer,
		callback,
		brokers,
		queueName,
		"test-group-id",
		sarama.NewConfig(),
	)
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
