package kafkaqueue

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
