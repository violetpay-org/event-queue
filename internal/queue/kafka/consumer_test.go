package kafkaqueue_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	kafkaqueue "github.com/violetpay-org/event-queue/internal/queue/kafka"
	"github.com/violetpay-org/event-queue/queue"
)

var (
	baseSerializeThenConsume queue.Callback[*sarama.ConsumerMessage] = func(msg *sarama.ConsumerMessage) error { return nil }
)

func TestNewConsumer(t *testing.T) {
	var consumer *kafkaqueue.Consumer
	t.Run("NewConsumer", func(t *testing.T) {
		t.Cleanup(func() {
			consumer = nil
		})

		consumer = kafkaqueue.NewAutoCommittingConsumer(baseSerializeThenConsume)
		assert.NotNil(t, consumer)
	})
}

func TestConsumer_Callback(t *testing.T) {
	var consumer *kafkaqueue.Consumer
	t.Run("Callback", func(t *testing.T) {
		t.Cleanup(func() {
			consumer = nil
		})

		consumer = kafkaqueue.NewAutoCommittingConsumer(baseSerializeThenConsume)
		assert.NotNil(t, consumer)

		assert.NotNil(t, consumer.Callback())
	})
}

func TestConsumer_ConsumeClaim(t *testing.T) {
	callbackCount := atomic.Int64{}
	callback := func(msg *sarama.ConsumerMessage) error {
		callbackCount.Add(1)
		return nil
	}
	consumer := kafkaqueue.NewAutoCommittingConsumer(callback)
	sess := &kafkaqueue.MockConsumerGroupSession{}
	msg := &kafkaqueue.MockConsumerGroupClaim{}

	t.Run("ConsumeClaim context canceled", func(t *testing.T) {
		t.Cleanup(func() {
			sess = &kafkaqueue.MockConsumerGroupSession{}
			msg = &kafkaqueue.MockConsumerGroupClaim{}
			callbackCount = atomic.Int64{}
			consumer = kafkaqueue.NewAutoCommittingConsumer(callback)
		})

		ctx, cancel := context.WithCancel(context.Background())
		sess.Ctx = ctx

		msg.DataChan = make(chan *sarama.ConsumerMessage, 1)
		defer close(msg.DataChan)

		go func() {
			time.Sleep(1 * time.Second)
			cancel()
		}()

		err := consumer.ConsumeClaim(sess, msg)
		assert.Nil(t, err)
	})

	t.Run("ConsumeClaim message channel is closed", func(t *testing.T) {
		t.Cleanup(func() {
			sess = &kafkaqueue.MockConsumerGroupSession{}
			msg = &kafkaqueue.MockConsumerGroupClaim{}
			callbackCount = atomic.Int64{}
			consumer = kafkaqueue.NewAutoCommittingConsumer(callback)
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sess.Ctx = ctx

		msg.DataChan = make(chan *sarama.ConsumerMessage, 1)

		go func() {
			time.Sleep(1 * time.Second)
			close(msg.DataChan)
		}()

		err := consumer.ConsumeClaim(sess, msg)
		assert.Nil(t, err)
	})

	t.Run("ConsumeClaim no problems", func(t *testing.T) {
		t.Cleanup(func() {
			sess = &kafkaqueue.MockConsumerGroupSession{}
			msg = &kafkaqueue.MockConsumerGroupClaim{}
			callbackCount = atomic.Int64{}
			consumer = kafkaqueue.NewAutoCommittingConsumer(callback)
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		sess.Ctx = ctx

		msg.DataChan = make(chan *sarama.ConsumerMessage, 1)

		go func() {
			time.Sleep(1 * time.Second)
			msg.DataChan <- &sarama.ConsumerMessage{
				Topic:     "test",
				Partition: 1,
				Key:       []byte("key"),
				Value:     []byte("value"),
				Offset:    0,
			}
			time.Sleep(1 * time.Second)
			close(msg.DataChan)
		}()

		assert.Equal(t, int64(0), callbackCount.Load())
		err := consumer.ConsumeClaim(sess, msg)
		assert.Nil(t, err)
		assert.Equal(t, int64(1), callbackCount.Load())
	})
}
