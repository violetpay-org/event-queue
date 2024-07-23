package kafkaqueue

import (
	"log"

	"github.com/IBM/sarama"
)

type ManualCommittingConsumer struct {
	callback ConsumerCallback
}

func NewManualCommittingConsumer(callback ConsumerCallback) sarama.ConsumerGroupHandler {
	return &ManualCommittingConsumer{
		callback: callback,
	}
}

func (c *ManualCommittingConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			err := c.callback(msg)
			if err == nil {
				session.Commit()
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (c *ManualCommittingConsumer) Setup(session sarama.ConsumerGroupSession) error {
	log.Print("Consumer up and running")
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (c *ManualCommittingConsumer) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Print("Consumer stopped")
	return nil
}

func (c *ManualCommittingConsumer) Callback() ConsumerCallback {
	return c.callback
}
