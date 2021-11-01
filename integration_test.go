package tankgo

import (
	"log"
	"testing"

	"github.com/TheBestCo/tankgo/message"
	"github.com/TheBestCo/tankgo/subscriber"
	"github.com/stretchr/testify/assert"
)

func TestIntegration(t *testing.T) {

	// f2 := message.ConsumeRequest{
	// 	ClientVersion: 2,
	// 	RequestID:     123,
	// 	Client:        "test_case_1",
	// 	MaxWaitMS:     0,
	// 	MinBytes:      0,
	// 	Topics: []message.FetchRequestTopic{
	// 		{
	// 			Name: "apache",
	// 			Partitions: []message.FetchRequestTopicPartition{
	// 				{
	// 					PartitionID:       0,
	// 					ABSSequenceNumber: 23245940571,
	// 					FetchSize:         2048,
	// 				},
	// 			},
	// 		},
	// 	},
	// }

	f2 := message.ConsumeRequest{
		ClientVersion: 2,
		RequestID:     123,
		Client:        "test_case_1",
		MaxWaitMS:     0,
		MinBytes:      0,
		Topics: []message.FetchRequestTopic{
			{
				Name: "topic1",
				Partitions: []message.FetchRequestTopicPartition{
					{
						PartitionID:       0,
						ABSSequenceNumber: 1,
						FetchSize:         2048,
					},
				},
			},
		},
	}

	s, err := subscriber.NewSubscriber("127.0.0.1:11011")
	//s, err := subscriber.NewSubscriber("192.168.10.235:11011")
	assert.NoError(t, err)

	err = s.Ping()
	assert.NoError(t, err)

	messages, err := s.Subscribe(&f2, 100)
	assert.NoError(t, err)

	log.Println("waiting for messages")

	for m := range messages {
		log.Printf("%s %s", m.Key, m.Payload)
	}
}
