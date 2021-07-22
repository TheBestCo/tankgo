// +build integration_test

package tankgo

import (
	"net"
	"testing"
)

func TestIntegration(t *testing.T) {
	t.Parallel()

	tcpAddr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:11011")
	if err != nil {
		t.Fatal(err)
	}

	c, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		t.Fatalf("Connection error %s", err.Error())
	}

	conn := NewConn(c)

	mt, _, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("error in basic header peek: %s", err.Error())
	}

	if mt != messageTypePing {
		t.Fatalf("expected ping got %d", mt)
	}

	f := ConsumeRequest{
		ClientVersion: 2,
		RequestID:     123,
		Client:        "test_case_1",
		MaxWaitMS:     0,
		MinBytes:      0,
		Topics: []FetchRequestTopic{
			{
				Name: "topic1",
				Partitions: []FetchRequestTopicPartition{
					{
						PartitionID:       0,
						ABSSequenceNumber: 1,
						FetchSize:         2048,
					},
				},
			},
		},
	}

	err = conn.Send(&f)
	if err != nil {
		t.Fatalf("error while sending message: %s", err.Error())
	}

	var msg interface{}
	mt, msg, err = conn.ReadMessage()

	if err != nil {
		t.Fatalf("error in basic header peek: %s", err.Error())
	}

	if mt != messageTypeConsume {
		t.Fatalf("expected consume response got %d", mt)
	}

	consumeResponse, ok := msg.(ConsumeResponse)
	if !ok {
		t.Fatalf("expected consume response got %#v", msg)
	}

	_ = consumeResponse
}
