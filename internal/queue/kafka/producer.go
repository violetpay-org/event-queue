package kafkaqueue

<<<<<<< HEAD
import "github.com/IBM/sarama"

type producerFromPool struct {
	syncProducer sarama.SyncProducer
	isReturned   bool
	isClosed     bool
	retractor    func() // returns the producer to the pool
	closer       func() // notifies the pool that the producer is closed
}

func NewProducerFromPool(
	brokers []string,
	config *sarama.Config,
	retractor func() error,
) (ReturnableSyncProducer, error) {
	producer, err := sarama.NewSyncProducer(
		brokers,
		config,
	)
=======
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
>>>>>>> feature/manual-commit
	if err != nil {
		return nil, err
	}

	return &producer{
		syncProducer: producer,
		isReturned:   false,
		retractor:    retractor,
	}, nil
}

func (p *producerFromPool) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	return p.syncProducer.SendMessage(msg)
}

func (p *producerFromPool) Close() (err error) {
	notifiesToPoolIfSuccessful := func() {
		if err != nil {
			return
		}

		p.isClosed = true
		p.closer()
	}

	if !p.isReturned && !p.isClosed {
		defer notifiesToPoolIfSuccessful()
		return p.syncProducer.Close()
	}

	return nil
}

func (p *producerFromPool) TxnStatus() sarama.ProducerTxnStatusFlag {
	return p.syncProducer.TxnStatus()
}

func (p *producerFromPool) Return() (err error) {
	if p.TxnStatus()&sarama.ProducerTxnFlagInError != 0 {
		return p.Close()
	}

	if !p.isReturned && !p.isClosed {
		p.isReturned = true
		p.retractor()
		return
	}
}
