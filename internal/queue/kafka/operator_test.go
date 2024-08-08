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
	return kafkaqueue.NewConsumer(
		serializer,
		callback,
		brokers,
		queueName,
		groupId,
		sarama.NewConfig(),
	)
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
	err := godotenv.Load("../../../.env")
	if err != nil {
		t.Error("Error loading .env file")
	}

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
