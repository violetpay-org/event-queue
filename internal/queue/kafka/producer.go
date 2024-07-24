package kafkaqueue

import (
	"fmt"
	"sync"

	"github.com/IBM/sarama"
)

// ProducerPool is a pool of producers that can be used to produce messages to Kafka for one set of brokers.
// It is not related to Transaction, Transactional Producer implements by configProvider.
type ProducerPool struct {
	locker    sync.Mutex
	producers []sarama.SyncProducer

	brokers        []string
	configProvider func() *sarama.Config
}

// Take returns a producer from pool. If the producer does not exist, it creates a new one.
func (p *ProducerPool) Take() (producer sarama.SyncProducer) {
	p.locker.Lock()
	defer p.locker.Unlock()

	if len(p.producers) == 0 {
		producer = p.generateProducer()
		return
	}

	producer = p.producers[0]
	p.producers = p.producers[1:]

	return
}

// Return returns a producer to the pool.
func (p *ProducerPool) Return(producer sarama.SyncProducer) {
	p.locker.Lock()
	defer p.locker.Unlock()

	// If the producer is closed, do not return it to the pool
	if producer == nil {
		return
	}

	// If the producer has an txError, do not return it to the pool
	if producer.TxnStatus()&sarama.ProducerTxnFlagInError != 0 {
		_ = producer.Close()
		return
	}

	p.producers = append(p.producers, producer)
}

func (p *ProducerPool) Producers() []sarama.SyncProducer {
	return p.producers
}

func (p *ProducerPool) Close() {
	p.locker.Lock()
	defer p.locker.Unlock()

	for _, producer := range p.producers {
		_ = producer.Close()
	}
}

func NewProducerPool(brokers []string, configProvider func() *sarama.Config) *ProducerPool {
	if configProvider() == nil {
		panic("configProvider is nil")
	}

	pool := &ProducerPool{
		locker:         sync.Mutex{},
		producers:      []sarama.SyncProducer{},
		brokers:        brokers,
		configProvider: configProvider,
	}

	return pool
}

func (p *ProducerPool) generateProducer() sarama.SyncProducer {
	producer, err := sarama.NewSyncProducer(p.brokers, p.configProvider())
	if err != nil {
		fmt.Println("Error creating producer", err)
		return nil
	}

	return producer
}
