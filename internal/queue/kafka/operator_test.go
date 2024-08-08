package kafkaqueue_test

import (
	"os"
	"testing"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
	kafkaqueue "github.com/violetpay-org/event-queue/internal/queue/kafka"
	"github.com/violetpay-org/event-queue/queue"
)

func mapOperatorProvider(
	queueName string,
	serializer queue.MessageSerializer[*sarama.ConsumerMessage, map[string]string],
	callback queue.Callback[map[string]string],
) queue.Consumer[*sarama.ConsumerMessage, map[string]string] {
	brokers, err := queue.GetBrokers(3)
	if err != nil {
		panic(err)
	}

	groupId := os.Getenv("KAFKA_GROUP_ID")
	return kafkaqueue.NewConsumeOperator(
		serializer,
		callback,
		brokers,
		queueName,
		groupId,
		sarama.NewConfig(),
	)
}

func rawMsgProvider() *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{}
}

func TestConsumeOperator(t *testing.T) {
	err := godotenv.Load("../../../.env")
	if err != nil {
		t.Error("Error loading .env file")
	}

	t.Run("map[string]string", func(t *testing.T) {
		queue.TestSuiteConsumeOperator[*sarama.ConsumerMessage, map[string]string](
			t,
			mapOperatorProvider,
			rawMsgProvider,
		)
	})
}
