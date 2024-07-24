package kafkaqueue

import (
	"log"
	"sync"

	"github.com/IBM/sarama"
)

// NOT YET IMPLEMENTED !!!
type ManualCommittingConsumer struct {
	callback ConsumerCallback

	messageQueue chan *sarama.ConsumerMessage
	mutex        sync.Mutex
}

func NewManualCommittingConsumer(callback ConsumerCallback) sarama.ConsumerGroupHandler {
	return &ManualCommittingConsumer{
		callback: callback,
	}
}

func (c *ManualCommittingConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	lst := make([]*sarama.ConsumerMessage, 10)
	mutex := sync.Mutex{}

	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			mutex.Lock()
			lst = append(lst, msg)
			mutex.Unlock()
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
