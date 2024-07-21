package queue_test

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/violetpay-org/event-queue/queue"
	"testing"
)

type testStruct struct {
	Key1 string `json:"key1"`
	Key2 string `json:"key2"`
	Key3 string `json:"myKey"`
}

func TestJSONSerializer_Serialize(t *testing.T) {
	t.Run("map[string]string serializer", func(t *testing.T) {

		var serializer queue.MessageSerializer[[]byte, map[string]string]

		t.Cleanup(func() {
			serializer = nil
		})

		t.Run("should return error when input is not a valid JSON", func(t *testing.T) {
			t.Cleanup(func() {
				serializer = nil
			})

			serializer = queue.NewJSONSerializer[map[string]string]()
			result, err := serializer.Serialize([]byte("invalid json"))

			assert.NotNil(t, err)
			assert.Nil(t, result)
		})

		t.Run("should return valid map when input is a valid JSON", func(t *testing.T) {
			t.Cleanup(func() {
				serializer = nil
			})

			serializer = queue.NewJSONSerializer[map[string]string]()
			result, err := serializer.Serialize([]byte(`{"key": "value"}`))

			assert.Nil(t, err)
			assert.Equal(t, map[string]string{"key": "value"}, result)
		})
	})

	t.Run("struct serializer", func(t *testing.T) {
		var serializer queue.MessageSerializer[[]byte, testStruct]

		t.Cleanup(func() {
			serializer = nil
		})

		t.Run("should return error when input is not a valid JSON", func(t *testing.T) {
			t.Cleanup(func() {
				serializer = nil
			})

			serializer = queue.NewJSONSerializer[testStruct]()
			result, err := serializer.Serialize([]byte("invalid json"))

			assert.NotNil(t, err)
			assert.Equal(t, testStruct{}, result)
		})

		t.Run("should return valid struct when input is a valid JSON", func(t *testing.T) {
			t.Cleanup(func() {
				serializer = nil
			})

			serializer = queue.NewJSONSerializer[testStruct]()
			result, err := serializer.Serialize([]byte(`{"key1": "value1", "key2": "value2", "myKey": "myValue"}`))

			assert.Nil(t, err)
			assert.Equal(t, testStruct{"value1", "value2", "myValue"}, result)
		})
	})
}

func TestKafkaJSONSerializer_Serialize(t *testing.T) {
	t.Run("map[string]string serializer", func(t *testing.T) {
		var serializer queue.MessageSerializer[*sarama.ConsumerMessage, map[string]string]

		t.Cleanup(func() {
			serializer = nil
		})

		t.Run("should return valid map when input is a valid JSON", func(t *testing.T) {
			t.Cleanup(func() {
				serializer = nil
			})

			serializer = queue.NewKafkaJSONSerializer[map[string]string]()
			result, err := serializer.Serialize(&sarama.ConsumerMessage{
				Value: []byte(`{"key": "value"}`),
			})

			assert.Nil(t, err)
			assert.Equal(t, map[string]string{"key": "value"}, result)
		})

		t.Run("should return error when input is not a valid JSON", func(t *testing.T) {
			t.Cleanup(func() {
				serializer = nil
			})

			serializer = queue.NewKafkaJSONSerializer[map[string]string]()
			result, err := serializer.Serialize(&sarama.ConsumerMessage{
				Value: []byte("invalid json"),
			})

			assert.NotNil(t, err)
			assert.Nil(t, result)
		})
	})

	t.Run("struct serializer", func(t *testing.T) {
		var serializer queue.MessageSerializer[*sarama.ConsumerMessage, testStruct]

		t.Run("should return valid struct when input is a valid JSON", func(t *testing.T) {
			t.Cleanup(func() {
				serializer = nil
			})

			serializer = queue.NewKafkaJSONSerializer[testStruct]()
			result, err := serializer.Serialize(&sarama.ConsumerMessage{
				Value: []byte(`{"key1": "value1", "key2": "value2", "myKey": "myValue"}`),
			})

			assert.Nil(t, err)
			assert.Equal(t, testStruct{"value1", "value2", "myValue"}, result)
		})

		t.Run("should return error when input is not a valid JSON", func(t *testing.T) {
			t.Cleanup(func() {
				serializer = nil
			})

			serializer = queue.NewKafkaJSONSerializer[testStruct]()
			result, err := serializer.Serialize(&sarama.ConsumerMessage{
				Value: []byte("invalid json"),
			})

			assert.NotNil(t, err)
			assert.Equal(t, testStruct{}, result)
		})
	})
}
