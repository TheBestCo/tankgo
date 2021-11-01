package tankgo

import (
	"fmt"
	"testing"

	"github.com/TheBestCo/tankgo/message"
	"github.com/TheBestCo/tankgo/subscriber"
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

	s, _ := subscriber.NewSubscriber("127.0.0.1:11011")
	//s, _ := subscriber.NewSubscriber("192.168.10.235:11011")

	s.Ping()
	messages, _ := s.Subscribe(&f2, 100)
	fmt.Println("waiting for messages")

	for m := range messages {
		s := fmt.Sprintf("%s %s", m.Key, m.Payload)
		fmt.Println(s)
	}

	t.Fatal()
}
