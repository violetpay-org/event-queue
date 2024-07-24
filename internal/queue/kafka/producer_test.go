package kafkaqueue_test

import (
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	kafkaqueue "github.com/violetpay-org/event-queue/internal/queue/kafka"
)

var pbrokers = []string{"kafka.vp-datacenter-1.violetpay.net:9092", "kafka.vp-datacenter-1.violetpay.net:9093", "kafka.vp-datacenter-1.violetpay.net:9094"}

func TestNewProducerPool(t *testing.T) {
	t.Run("NewProducerPool no configProvider", func(t *testing.T) {
		assert.Panics(t, func() {
			_ = kafkaqueue.NewProducerPool(pbrokers, nil)
		})

		assert.Panics(t, func() {
			_ = kafkaqueue.NewProducerPool(pbrokers, func() *sarama.Config {
				return nil
			})
		})
	})
}

func TestProducerPool_Close(t *testing.T) {
	pool := kafkaqueue.NewProducerPool(pbrokers, func() *sarama.Config {
		return sarama.NewConfig()
	})

	producer := pool.Take()
	assert.NotNil(t, &producer)

	pool.Return(producer)

	assert.NotPanics(t, func() {
		pool.Close()
	})
}

func TestProducerPool_Take(t *testing.T) {
	pool := kafkaqueue.NewProducerPool(pbrokers, func() *sarama.Config {
		return sarama.NewConfig()
	})
	var producer sarama.SyncProducer

	t.Run("Take", func(t *testing.T) {
		t.Cleanup(func() {
			pool = kafkaqueue.NewProducerPool(pbrokers, func() *sarama.Config {
				return sarama.NewConfig()
			})
		})

		producer = pool.Take()
		assert.NotNil(t, &producer)

		producers := pool.Producers()
		assert.NotNil(t, &producers)

		assert.Equal(t, 0, len(producers))
	})

	t.Run("Take with error when generate producer", func(t *testing.T) {
		t.Cleanup(func() {
			pool = kafkaqueue.NewProducerPool(pbrokers, func() *sarama.Config {
				return sarama.NewConfig()
			})
		})

		pool = kafkaqueue.NewProducerPool(pbrokers, func() *sarama.Config {
			return &sarama.Config{}
		})

		producer = pool.Take()
		assert.Nil(t, producer)
	})
}

func TestProducerPool_Return(t *testing.T) {
	pool := kafkaqueue.NewProducerPool(pbrokers, func() *sarama.Config {
		return sarama.NewConfig()
	})

	t.Run("Return", func(t *testing.T) {
		t.Cleanup(func() {
			pool = kafkaqueue.NewProducerPool(pbrokers, func() *sarama.Config {
				return sarama.NewConfig()
			})
		})

		producer := pool.Take()
		assert.NotNil(t, &producer)

		pool.Return(producer)

		producers := pool.Producers()
		assert.NotNil(t, &producers)

		assert.Equal(t, 1, len(producers))
		pool.Return(nil)
		assert.Equal(t, 1, len(producers))

		producer = pool.Take()
		assert.NotNil(t, &producer)

		producers = pool.Producers()
		assert.Equal(t, 0, len(producers))
	})

	//t.Run("Return closed producer", func(t *testing.T) {
	//	assert.Equal(t, 0, len(producers[topic]))
	//	producer = pool.Take(topic)
	//	err := producer.Close()
	//	assert.Nil(t, err)
	//	err = producer.Close()
	//	assert.Nil(t, err)
	//	producer.Errors()
	//
	//	pool.Return(producer, topic)
	//	assert.Equal(t, 0, len(producers[topic]))
	//})

	t.Run("Return txError producer", func(t *testing.T) {
		t.Cleanup(func() {
			pool = kafkaqueue.NewProducerPool(pbrokers, func() *sarama.Config {
				return sarama.NewConfig()
			})
		})

		producer := kafkaqueue.NewMockSyncProducer()
		producers := pool.Producers()

		pool.Return(producer)
		assert.Equal(t, 0, len(producers))
		// assert.Equal(t, 1, producer.CloseCalled)
	})
}
