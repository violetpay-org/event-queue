package kafkaqueue

import (
	"log"

	"github.com/IBM/sarama"
)

// Consumer implements sarama.ConsumerGroupHandler.
type ConsumerGroupHandlerForAutoCommit struct {
	callback ConsumerCallback
}

// ConsumeClaim must start a Consumer loop of ConsumerGroupClaim's Messages().
// NOTE: This must not be called within a goroutine, already handled as goroutines by sarama.
func (c *ConsumerGroupHandlerForAutoCommit) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			c.callback(msg)
		case <-session.Context().Done():
			return nil
		}
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (c *ConsumerGroupHandlerForAutoCommit) Setup(session sarama.ConsumerGroupSession) error {
	log.Print("Consumer up and running")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (c *ConsumerGroupHandlerForAutoCommit) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Print("Consumer stopped")
	return nil
}

func (c *ConsumerGroupHandlerForAutoCommit) Callback() ConsumerCallback {
	return c.callback
}
