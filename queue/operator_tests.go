package queue

import (
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
)

type mockSerializer[Input any, Output any] struct {
	count atomic.Int64
}

func (s *mockSerializer[Input, Output]) Serialize(input Input) (o Output, err error) {
	s.count.Add(1)
	return
}

func (s *mockSerializer[Input, Output]) Count() int64 {
	return s.count.Load()
}

func newMockSerializer[Input any, Output any]() *mockSerializer[Input, Output] {
	return &mockSerializer[Input, Output]{
		count: atomic.Int64{},
	}
}

func TestSuiteConsumeOperator[InMsg any, Msg any](
	t *testing.T,
	operatorProvider func(queueName string, serializer MessageSerializer[InMsg, Msg], callback Callback[Msg]) ConsumeOperator[InMsg, Msg],
	rawMessageProvider func() InMsg,
) {
	t.Run("ConsumeOperator Test Suite", func(t *testing.T) {
		var queueName string
		var serializer MessageSerializer[InMsg, Msg]
		var callbackCount atomic.Int64
		var callbackValue Msg
		var callback Callback[Msg]

		queueName = "testQueueName"
		serializer = newMockSerializer[InMsg, Msg]()
		callbackCount = atomic.Int64{}
		callback = func(msg Msg) {
			callbackCount.Add(1)
		}

		operator := operatorProvider(queueName, serializer, callback)

		t.Run("QueueName", func(t *testing.T) {
			t.Cleanup(func() {
				queueName = "testQueueName"
				serializer = newMockSerializer[InMsg, Msg]()
				callbackCount = atomic.Int64{}
				callback = func(msg Msg) {
					callbackCount.Add(1)
				}
				operator = operatorProvider(queueName, serializer, callback)
			})

			assert.Equal(t, operator.QueueName(), queueName)
		})

		t.Run("Serializer", func(t *testing.T) {
			t.Cleanup(func() {
				queueName = "testQueueName"
				serializer = newMockSerializer[InMsg, Msg]()
				callbackCount = atomic.Int64{}
				callback = func(msg Msg) {
					callbackCount.Add(1)
				}
				operator = operatorProvider(queueName, serializer, callback)
			})

			assert.NotNil(t, operator.Serializer(), serializer)
			value, err := serializer.Serialize(rawMessageProvider())
			if err != nil {
				return
			}

			assert.NotNil(t, value)
			assert.Nil(t, err)
		})

		t.Run("Callback", func(t *testing.T) {
			t.Cleanup(func() {
				queueName = "testQueueName"
				serializer = newMockSerializer[InMsg, Msg]()
				callbackCount = atomic.Int64{}
				callback = func(msg Msg) {
					callbackCount.Add(1)
				}
				operator = operatorProvider(queueName, serializer, callback)
			})

			assert.NotNil(t, operator.Callback())
		})

		t.Run("Consume", func(t *testing.T) {
			t.Cleanup(func() {
				queueName = "testQueueName"
				serializer = newMockSerializer[InMsg, Msg]()
				callbackCount = atomic.Int64{}
				callback = func(msg Msg) {
					callbackCount.Add(1)
					callbackValue = msg
				}
				operator = operatorProvider(queueName, serializer, callback)
			})

			msg := rawMessageProvider()
			assert.NotNil(t, msg)

			expectedSerializedValue, err := serializer.Serialize(msg)
			assert.Nil(t, err)

			operator.Consume(msg)
			assert.Equal(t, serializer.(*mockSerializer[InMsg, Msg]).Count(), int64(1))
			assert.Equal(t, callbackCount.Load(), int64(1))
			assert.Equal(t, callbackValue, expectedSerializedValue)
		})

		t.Run("StartConsume", func(t *testing.T) {
			t.Cleanup(func() {
				queueName = "testQueueName"
				serializer = newMockSerializer[InMsg, Msg]()
				callbackCount = atomic.Int64{}
				callback = func(msg Msg) {
					callbackCount.Add(1)
				}
				operator.StopConsume()
				operator = operatorProvider(queueName, serializer, callback)
			})

			assert.NotPanics(t, func() {
				operator.StartConsume()
			})

		})

		t.Run("StopConsume", func(t *testing.T) {
			t.Cleanup(func() {
				queueName = "testQueueName"
				serializer = newMockSerializer[InMsg, Msg]()
				callbackCount = atomic.Int64{}
				callback = func(msg Msg) {
					callbackCount.Add(1)
				}
				operator.StopConsume()
				operator = operatorProvider(queueName, serializer, callback)
			})

			assert.NotPanics(t, func() {
				operator.StartConsume()
			})

			assert.NotPanics(t, func() {
				operator.StopConsume()
			})
		})
	})
}
