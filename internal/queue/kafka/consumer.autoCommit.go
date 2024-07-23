package kafkaqueue

import (
	"log"

	"github.com/IBM/sarama"
	"github.com/violetpay-org/event-queue/queue"
)

type ConsumerCallback = queue.Callback[*sarama.ConsumerMessage]

// Consumer implements sarama.ConsumerGroupHandler.
type Consumer struct {
	callback ConsumerCallback
}

func NewAutoCommittingConsumer(callback ConsumerCallback) *Consumer {
	return &Consumer{
		callback: callback,
	}
}

// ConsumeClaim must start a Consumer loop of ConsumerGroupClaim's Messages().
// NOTE: This must not be called within a goroutine, already handled as goroutines by sarama.
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			go c.callback(msg)
		case <-session.Context().Done():
			return nil
		}
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (c *Consumer) Setup(session sarama.ConsumerGroupSession) error {
	log.Print("Consumer up and running")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (c *Consumer) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Print("Consumer stopped")
	return nil
}

func (c *Consumer) Callback() ConsumerCallback {
	return c.callback
}
