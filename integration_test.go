package tankgo

import (
	"context"
	"testing"

	"github.com/TheBestCo/tankgo/message"
	"github.com/TheBestCo/tankgo/subscriber"
	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type TankContainer struct {
	testcontainers.Container
	Port nat.Port
}

func setupTankContainer(ctx context.Context) (*TankContainer, error) {
	req := testcontainers.ContainerRequest{
		Image:        "phaistos/tank",
		ExposedPorts: []string{"11011"},
		WaitingFor: wait.ForAll(
			wait.ForLog("1 topics registered"),
			wait.ForListeningPort("11011/tcp"),
		),
		Cmd: []string{
			"/bin/sh", "-c",
			"mkdir -p /data/test_topic/0; tank -p /data -l :11011",
		},
	}

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	p, err := c.MappedPort(ctx, "11011")
	if err != nil {
		return nil, err
	}

	return &TankContainer{Container: c, Port: p}, nil
}

func TestIntegration(t *testing.T) {
	ctx := context.Background()

	tankC, err := setupTankContainer(ctx)
	assert.NoError(t, err)
	defer tankC.Terminate(ctx)

	// Send message
	_, err = tankC.Exec(ctx, []string{"/usr/local/bin/tank-cli", "-t", "test_topic", "produce", "one", "two", "three"})
	assert.NoError(t, err)

	req := message.ConsumeRequest{
		ClientVersion: 2,
		RequestID:     123,
		Client:        "test_case_1",
		MaxWaitMS:     0,
		MinBytes:      0,
		Topics: []message.FetchRequestTopic{
			{
				Name: "test_topic",
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

	s, err := subscriber.NewSubscriber("127.0.0.1:" + tankC.Port.Port())

	assert.NoError(t, err)

	defer s.Close()

	err = s.Ping()
	assert.NoError(t, err)

	messages, errChan := s.Subscribe(&req, 50)
	assert.NoError(t, err)

	msgList := make([]message.MessageLog, 0, 50)

loop:
	for {
		select {
		case err = <-errChan:
			if err != nil {
				t.Fatal(err)
				break loop
			}
		default:
			for m := range messages {
				msgList = append(msgList, m)
			}
			break loop
		}
	}

	assert.True(t, len(msgList) == 3)
	assert.Equal(t, "one", string(msgList[0].Payload))
	assert.Equal(t, "two", string(msgList[1].Payload))
	assert.Equal(t, "three", string(msgList[2].Payload))
}
