package kafkaqueue_test

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/violetpay-org/event-queue/internal/queue/kafka"
	"testing"
)

func TestMockConsumerGroupSession_Claims(t *testing.T) {
	var sess *kafkaqueue.MockConsumerGroupSession

	t.Run("Claims", func(t *testing.T) {
		t.Cleanup(func() {
			sess = nil
		})

		sess = &kafkaqueue.MockConsumerGroupSession{}
		assert.Nil(t, sess.Claims())
	})
}

func TestMockConsumerGroupSession_MemberID(t *testing.T) {
	var sess *kafkaqueue.MockConsumerGroupSession

	t.Run("MemberID", func(t *testing.T) {
		t.Cleanup(func() {
			sess = nil
		})

		sess = &kafkaqueue.MockConsumerGroupSession{}
		assert.Equal(t, "", sess.MemberID())
	})
}

func TestMockConsumerGroupSession_GenerationID(t *testing.T) {
	var sess *kafkaqueue.MockConsumerGroupSession

	t.Run("GenerationID", func(t *testing.T) {
		t.Cleanup(func() {
			sess = nil
		})

		sess = &kafkaqueue.MockConsumerGroupSession{}
		assert.Equal(t, int32(0), sess.GenerationID())
	})
}

func TestMockConsumerGroupSession_MarkOffset(t *testing.T) {
	var sess *kafkaqueue.MockConsumerGroupSession

	t.Run("MarkOffset", func(t *testing.T) {
		t.Cleanup(func() {
			sess = nil
		})

		sess = &kafkaqueue.MockConsumerGroupSession{}
		sess.MarkOffset("", 0, 0, "")
	})
}

func TestMockConsumerGroupSession_Commit(t *testing.T) {
	var sess *kafkaqueue.MockConsumerGroupSession

	t.Run("Commit", func(t *testing.T) {
		t.Cleanup(func() {
			sess = nil
		})

		sess = &kafkaqueue.MockConsumerGroupSession{}
		sess.Commit()
	})
}

func TestMockConsumerGroupSession_ResetOffset(t *testing.T) {
	var sess *kafkaqueue.MockConsumerGroupSession

	t.Run("ResetOffset", func(t *testing.T) {
		t.Cleanup(func() {
			sess = nil
		})

		sess = &kafkaqueue.MockConsumerGroupSession{}
		sess.ResetOffset("", 0, 0, "")
		assert.Equal(t, 1, sess.ResetOffsetCalled)
	})

	t.Run("ResetOffset multiple times", func(t *testing.T) {
		t.Cleanup(func() {
			sess = nil
		})

		sess = &kafkaqueue.MockConsumerGroupSession{}
		sess.ResetOffset("", 0, 0, "")
		sess.ResetOffset("", 0, 0, "")
		assert.Equal(t, 2, sess.ResetOffsetCalled)
	})
}

func TestMockConsumerGroupSession_MarkMessage(t *testing.T) {
	var sess *kafkaqueue.MockConsumerGroupSession

	t.Run("MarkMessage", func(t *testing.T) {
		t.Cleanup(func() {
			sess = nil
		})

		sess = &kafkaqueue.MockConsumerGroupSession{}
		sess.MarkMessage(nil, "")
	})
}

func TestMockConsumerGroupSession_Context(t *testing.T) {
	var sess *kafkaqueue.MockConsumerGroupSession

	t.Run("Context", func(t *testing.T) {
		t.Cleanup(func() {
			sess = nil
		})

		sess = &kafkaqueue.MockConsumerGroupSession{}
		assert.Nil(t, sess.Context())
	})

	t.Run("Context not nil", func(t *testing.T) {
		t.Cleanup(func() {
			sess = nil
		})

		ctx := context.Background()
		sess = &kafkaqueue.MockConsumerGroupSession{Ctx: ctx}
		assert.NotNil(t, sess.Context())
		assert.Equal(t, ctx, sess.Context())
	})
}

func TestMockConsumerGroupClaim_Topic(t *testing.T) {
	var claim *kafkaqueue.MockConsumerGroupClaim

	t.Run("Topic", func(t *testing.T) {
		t.Cleanup(func() {
			claim = nil
		})

		claim = &kafkaqueue.MockConsumerGroupClaim{}
		assert.Equal(t, "", claim.Topic())
	})
}

func TestMockConsumerGroupClaim_Partition(t *testing.T) {
	var claim *kafkaqueue.MockConsumerGroupClaim

	t.Run("Partition", func(t *testing.T) {
		t.Cleanup(func() {
			claim = nil
		})

		claim = &kafkaqueue.MockConsumerGroupClaim{}
		assert.Equal(t, int32(0), claim.Partition())
	})
}

func TestMockConsumerGroupClaim_InitialOffset(t *testing.T) {
	var claim *kafkaqueue.MockConsumerGroupClaim

	t.Run("InitialOffset", func(t *testing.T) {
		t.Cleanup(func() {
			claim = nil
		})

		claim = &kafkaqueue.MockConsumerGroupClaim{}
		assert.Equal(t, int64(0), claim.InitialOffset())
	})
}

func TestMockConsumerGroupClaim_HighWaterMarkOffset(t *testing.T) {
	var claim *kafkaqueue.MockConsumerGroupClaim

	t.Run("HighWaterMarkOffset", func(t *testing.T) {
		t.Cleanup(func() {
			claim = nil
		})

		claim = &kafkaqueue.MockConsumerGroupClaim{}
		assert.Equal(t, int64(0), claim.HighWaterMarkOffset())
	})
}

func TestMockConsumerGroupClaim_Messages(t *testing.T) {
	var claim *kafkaqueue.MockConsumerGroupClaim

	t.Run("Messages", func(t *testing.T) {
		t.Cleanup(func() {
			claim = nil
		})

		claim = &kafkaqueue.MockConsumerGroupClaim{}
		assert.Nil(t, claim.Messages())
	})

	t.Run("Messages not nil, chan closed", func(t *testing.T) {
		t.Cleanup(func() {
			claim = nil
		})

		claim = &kafkaqueue.MockConsumerGroupClaim{DataChan: make(chan *sarama.ConsumerMessage)}
		assert.NotNil(t, claim.Messages())

		close(claim.DataChan)

		_, ok := <-claim.DataChan
		assert.False(t, ok)
	})
}

func TestMockAsyncProducer_AsyncClose(t *testing.T) {
	var producer *kafkaqueue.MockAsyncProducer

	t.Run("AsyncClose", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{}
		producer.AsyncClose()
	})

}

func TestMockAsyncProducer_Close(t *testing.T) {
	var producer *kafkaqueue.MockAsyncProducer

	t.Run("Close", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{}
		err := producer.Close()
		assert.Nil(t, err)
		assert.Equal(t, 1, producer.CloseCalled)
	})

	t.Run("Close multiple times", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{}
		err := producer.Close()
		assert.Nil(t, err)
		assert.Equal(t, 1, producer.CloseCalled)

		err = producer.Close()
		assert.Nil(t, err)
		assert.Equal(t, 2, producer.CloseCalled)
	})
}

func TestMockAsyncProducer_Input(t *testing.T) {
	var producer *kafkaqueue.MockAsyncProducer

	t.Run("Input", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{}
		assert.Nil(t, producer.Input())

		producer.InputChan = make(chan *sarama.ProducerMessage)
		assert.NotNil(t, producer.Input())

		close(producer.InputChan)

		_, ok := <-producer.InputChan
		assert.False(t, ok)
	})
}

func TestMockAsyncProducer_Successes(t *testing.T) {
	var producer *kafkaqueue.MockAsyncProducer

	t.Run("Successes", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{}
		assert.Nil(t, producer.Successes())
	})
}

func TestMockAsyncProducer_Errors(t *testing.T) {
	var producer *kafkaqueue.MockAsyncProducer

	t.Run("Errors", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{}
		assert.Nil(t, producer.Errors())
	})
}

func TestMockAsyncProducer_IsTransactional(t *testing.T) {
	var producer *kafkaqueue.MockAsyncProducer

	t.Run("IsTransactional", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{}
		assert.False(t, producer.IsTransactional())
	})
}

func TestMockAsyncProducer_TxnStatus(t *testing.T) {
	var producer *kafkaqueue.MockAsyncProducer

	t.Run("TxnStatus", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{}
		assert.Zero(t, producer.TxnStatus())

		producer.TxnStatusFlag = sarama.ProducerTxnFlagInError
		assert.NotZero(t, producer.TxnStatus())
		assert.Equal(t, sarama.ProducerTxnFlagInError, producer.TxnStatus())

		producer.TxnStatusFlag = sarama.ProducerTxnFlagAbortableError
		assert.NotZero(t, producer.TxnStatus())
		assert.Equal(t, sarama.ProducerTxnFlagAbortableError, producer.TxnStatus())
	})
}

func TestMockAsyncProducer_BeginTxn(t *testing.T) {
	var producer *kafkaqueue.MockAsyncProducer

	t.Run("BeginTxn", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{}
		err := producer.BeginTxn()
		assert.Nil(t, err)
		assert.Equal(t, 1, producer.BeginTxnCalled)
	})

	t.Run("BeginTxn with error", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{BeginTxnError: sarama.ErrOutOfBrokers}
		err := producer.BeginTxn()
		assert.NotNil(t, err)
		assert.Equal(t, 1, producer.BeginTxnCalled)
	})

	t.Run("BeginTxn with error multiple times", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{BeginTxnError: sarama.ErrOutOfBrokers}
		for i := 0; i < 9; i++ {
			err := producer.BeginTxn()
			assert.NotNil(t, err)
			assert.Equal(t, i+1, producer.BeginTxnCalled)
		}

		err := producer.BeginTxn()
		assert.Nil(t, err)
	})
}

func TestMockAsyncProducer_CommitTxn(t *testing.T) {
	var producer *kafkaqueue.MockAsyncProducer

	t.Run("CommitTxn", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{}
		err := producer.CommitTxn()
		assert.Nil(t, err)
		assert.Equal(t, 1, producer.CommitTxnCalled)
	})

	t.Run("CommitTxn with error", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{CommitTxnError: sarama.ErrOutOfBrokers}
		err := producer.CommitTxn()
		assert.NotNil(t, err)
		assert.Equal(t, 1, producer.CommitTxnCalled)
	})

	t.Run("CommitTxn with error multiple times", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{CommitTxnError: sarama.ErrOutOfBrokers}
		for i := 0; i < 9; i++ {
			err := producer.CommitTxn()
			assert.NotNil(t, err)
			assert.Equal(t, i+1, producer.CommitTxnCalled)
		}

		err := producer.CommitTxn()
		assert.Nil(t, err)
	})
}

func TestMockAsyncProducer_AbortTxn(t *testing.T) {
	var producer *kafkaqueue.MockAsyncProducer

	t.Run("AbortTxn", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{}
		err := producer.AbortTxn()
		assert.Nil(t, err)
		assert.Equal(t, 1, producer.AbortTxnCalled)
	})

	t.Run("AbortTxn with error", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{AbortTxnError: sarama.ErrOutOfBrokers}
		err := producer.AbortTxn()
		assert.NotNil(t, err)
		assert.Equal(t, 1, producer.AbortTxnCalled)
	})

	t.Run("AbortTxn with error multiple times", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{AbortTxnError: sarama.ErrOutOfBrokers}
		for i := 0; i < 9; i++ {
			err := producer.AbortTxn()
			assert.NotNil(t, err)
			assert.Equal(t, i+1, producer.AbortTxnCalled)
		}

		err := producer.AbortTxn()
		assert.Nil(t, err)
	})
}

func TestMockAsyncProducer_AddOffsetsToTxn(t *testing.T) {
	var producer *kafkaqueue.MockAsyncProducer

	t.Run("AddOffsetsToTxn", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{}
		err := producer.AddOffsetsToTxn(nil, "")
		assert.Nil(t, err)
	})
}

func TestMockAsyncProducer_AddMessageToTxn(t *testing.T) {
	var producer *kafkaqueue.MockAsyncProducer

	t.Run("AddMessageToTxn", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{}
		err := producer.AddMessageToTxn(nil, "", nil)
		assert.Nil(t, err)
		assert.Equal(t, 1, producer.AddMessageToTxnCalled)
	})

	t.Run("AddMessageToTxn with error", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{AddMessageToTxnError: sarama.ErrOutOfBrokers}
		err := producer.AddMessageToTxn(nil, "", nil)
		assert.NotNil(t, err)
		assert.Equal(t, 1, producer.AddMessageToTxnCalled)
	})

	t.Run("AddMessageToTxn with error multiple times", func(t *testing.T) {
		t.Cleanup(func() {
			producer = nil
		})

		producer = &kafkaqueue.MockAsyncProducer{AddMessageToTxnError: sarama.ErrOutOfBrokers}
		for i := 0; i < 9; i++ {
			err := producer.AddMessageToTxn(nil, "", nil)
			assert.NotNil(t, err)
			assert.Equal(t, i+1, producer.AddMessageToTxnCalled)
		}

		err := producer.AddMessageToTxn(nil, "", nil)
		assert.Nil(t, err)
	})
}

func TestMockConsumerGroupHandler_Setup(t *testing.T) {
	var handler *kafkaqueue.MockConsumerGroupHandler

	t.Run("Setup", func(t *testing.T) {
		t.Cleanup(func() {
			handler = nil
		})

		handler = &kafkaqueue.MockConsumerGroupHandler{}
		err := handler.Setup(nil)
		assert.Nil(t, err)
		assert.Equal(t, 1, handler.SetupCalled)
	})

	t.Run("Setup multiple times", func(t *testing.T) {
		t.Cleanup(func() {
			handler = nil
		})

		handler = &kafkaqueue.MockConsumerGroupHandler{}
		for i := 0; i < 10; i++ {
			err := handler.Setup(nil)
			assert.Nil(t, err)
			assert.Equal(t, i+1, handler.SetupCalled)
		}
	})
}

func TestMockConsumerGroupHandler_Cleanup(t *testing.T) {
	var handler *kafkaqueue.MockConsumerGroupHandler

	t.Run("Cleanup", func(t *testing.T) {
		t.Cleanup(func() {
			handler = nil
		})

		handler = &kafkaqueue.MockConsumerGroupHandler{}
		err := handler.Cleanup(nil)
		assert.Nil(t, err)
		assert.Equal(t, 1, handler.CleanupCalled)
	})

	t.Run("Cleanup multiple times", func(t *testing.T) {
		t.Cleanup(func() {
			handler = nil
		})

		handler = &kafkaqueue.MockConsumerGroupHandler{}
		for i := 0; i < 10; i++ {
			err := handler.Cleanup(nil)
			assert.Nil(t, err)
			assert.Equal(t, i+1, handler.CleanupCalled)
		}
	})
}

func TestMockConsumerGroupHandler_ConsumeClaim(t *testing.T) {
	var handler *kafkaqueue.MockConsumerGroupHandler

	t.Run("ConsumeClaim", func(t *testing.T) {
		t.Cleanup(func() {
			handler = nil
		})

		handler = &kafkaqueue.MockConsumerGroupHandler{}
		err := handler.ConsumeClaim(nil, nil)
		assert.Nil(t, err)
		assert.Equal(t, 1, handler.ConsumeClaimCalled)
	})

	t.Run("ConsumeClaim with error", func(t *testing.T) {
		t.Cleanup(func() {
			handler = nil
		})

		handler = &kafkaqueue.MockConsumerGroupHandler{ConsumeClaimError: sarama.ErrOutOfBrokers}
		err := handler.ConsumeClaim(nil, nil)
		assert.NotNil(t, err)
		assert.Equal(t, 1, handler.ConsumeClaimCalled)
	})

	t.Run("ConsumeClaim with error multiple times", func(t *testing.T) {
		t.Cleanup(func() {
			handler = nil
		})

		handler = &kafkaqueue.MockConsumerGroupHandler{ConsumeClaimError: sarama.ErrOutOfBrokers}
		for i := 0; i < 10; i++ {
			err := handler.ConsumeClaim(nil, nil)
			assert.NotNil(t, err)
			assert.Equal(t, i+1, handler.ConsumeClaimCalled)
		}
	})
}
