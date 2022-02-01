# TankGo

![image](https://user-images.githubusercontent.com/684680/151995604-13e90fae-ea97-46c1-86c3-60a47e5cafc9.png)

## Overview
TankGo is a stream processing library client for [Tank](https://github.com/phaistos-networks/TANK) written in Go. The project is still under development and until the latest [release](https://github.com/TheBestCo/tankgo/releases/tag/v0.1.8), some features like publishing messages are still missing. Currently only consuming from Tank server is supported. You can find more details regarding a high overview of the Tank protocol [here](https://github.com/phaistos-networks/TANK/blob/master/tank_protocol.md).
Also for more details regarding the Tank encoding scheme, which is the same that TankGo client follows refer to this [document](https://github.com/phaistos-networks/TANK/blob/master/tank_encoding.md).

## Basics on how to use TankGo client.
The basic struct your program will need to use in order to start communicating with Tank server is the [`TankSubscriber`](https://github.com/TheBestCo/tankgo/blob/8a3cb532dfc59bf4b0bfbff8a2e45ae7c2ad6135/subscriber.go#L40). And the high-level API that this structs implements can be found [here](https://github.com/TheBestCo/tankgo/blob/8a3cb532dfc59bf4b0bfbff8a2e45ae7c2ad6135/subscriber.go#L16).

``` go
type Subscriber interface {
	Connect(ctx context.Context, broker string, connectTimeout time.Duration, bufsize int) error
	Subscribe(r *message.ConsumeRequest, maxConcurrentReads int) (<-chan message.Log, <-chan error)
	Reset(ctx context.Context) error
	GetTopicsHighWaterMark(r *message.ConsumeRequest) (map[string]uint64, error)
	Ping() error
	Close() error
}
```

## Connecting with Tank
In order to connect with Tank you should just create a new `TankSubscriber` and call its `Connect` method like this example.
``` go
s := &TankSubscriber{}
err := s.Connect(ctx, "ip:port", DefaultConTimeout, DefaultBufSize)
```
`Connect` method uses a context as its first argument that is used if you need to handle the cancel of the initial underlying TCP connection with the Tank broker before `connectTimeout`.

The `connectTimeout` argument sets how long the `Connection` should try to establish the underlying TCP connection before returning an error.

The last argument `bufsize` sets the size of the internal read buffer.

## Consuming messages
On a successful connection and in order to start consuming messages from Tank you can now construct a new consume request described [here](https://github.com/phaistos-networks/TANK/blob/master/tank_protocol.md#fetchreq). The previous document describes in detail what kind of values the consume request can have and how to use them in some special cases.

The respective TankGo struct is the `ConsumeRequest` struct which can be found [here](https://github.com/TheBestCo/tankgo/blob/8a3cb532dfc59bf4b0bfbff8a2e45ae7c2ad6135/message/msgconsume.go#L83). An example of a new consume request is:
``` go
req := message.ConsumeRequest{
		ClientVersion: 1,
		RequestID:     1,
		Client:        "my_go_client",
		MaxWaitMS:     0,
		MinBytes:      0,
		Topics: []message.FetchRequestTopic{
			{
				Name: "test_topic",
				Partitions: []message.FetchRequestTopicPartition{
					{
						PartitionID:       0,
						ABSSequenceNumber: 1,
						FetchSize:         1024,
					},
				},
			},
		},
	}
```


your program should always call `Ping()` method and on succes call the `Subscribe(r *message.ConsumeRequest, maxConcurrentReads int) (<-chan message.Log, <-chan error)` method like this:

```go 
err := s.Ping()
// Check err here and on success continue.
messages, errChan := s.Subscribe(&req, 100)
```
Where `req` is the consume request and `maxConcurrentReads` sets the size of the buffered size returned.
Because TankGo is channel-based it is important your program should take into account all the pros and cons a channel-based approach can have.
For example, if the consumer blocks frequently and the channel is full that means the TankGo client will not be able to push new messages and will eventually slow down reading from the underlying stream. So it is very important that your program should take that into account and try to never block the gorouting that consumes from the returned channel.

Another important thing to notice is that `Subscribe` returns also an error channel that should be also checked for errors if any. A good approach would be to range over the `messages` channel and when this is over check the error in the `errChan`. It will always have a value even if it is a nil one.

## Important notes
- Always call `defer Close()` to close the underlying connection with Tank.
- On a successful consume request Tank server will start streaming all the available data specific for this request and it will expect to be consumed from the client. If the client exits early before consuming everything a new consume request to Tank will result to an error response.
- To handle such cases the `Reset(ctx context.Context)` can be used.
- The method `GetTopicsHighWaterMark(r *message.ConsumeRequest)` is a hacky way to get the sequence number of the latest committed of the specified topic/partition. Because it returns early without consuming all the responses from Tank a call to `Reset` is needed afterward.
