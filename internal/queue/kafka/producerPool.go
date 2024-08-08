package kafkaqueue

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/IBM/sarama"
)

type ReturnableSyncProducer interface {
	sarama.SyncProducer
	Return() error
}

type ProducerPoolConfig struct {
	Brokers        []string
	NumMaxConn     int
	ConfigProvider func(...interface{}) *sarama.Config
}

// ProducerPool is a pool of producers that can be used to produce messages to Kafka for one set of brokers.
// It is not related to Transaction, Transactional Producer implements by configProvider.
type ProducerPool struct {
	producers     chan ReturnableSyncProducer
	config        *ProducerPoolConfig
	numClosedConn atomic.Int32 // number of closed connections (bounded by config.NumMaxConn)
}

// Take returns a producer from pool. If the producer does not exist, it creates a new one.
func (p *ProducerPool) Take() (producer ReturnableSyncProducer) {
	return <-p.producers
}

// Return returns a producer to the pool.
func (p *ProducerPool) Return(producer ReturnableSyncProducer) {
	// If the producer is closed, do not return it to the pool
	if producer == nil {
		return
	}

	// If the producer has an txError, do not return it to the pool
	if producer.TxnStatus()&sarama.ProducerTxnFlagInError != 0 {
		_ = producer.Close()
		return
	}

	p.producers <- producer
}

func (p *ProducerPool) Close() {
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

func (p *ProducerPool) currentRunningProducers() int {
	return p.config.NumMaxConn - int(p.numClosedConn.Load())
}
