package tankgo

// MessageType alias.
type MessageType uint8

// Message types id.
const (
	messageTypeUnknown            MessageType = 0
	messageTypeConsume            MessageType = 2
	messageTypeProduce            MessageType = 1
	messageTypeProduce5           MessageType = 5
	messageTypePing               MessageType = 3
	messageTypeDiscoverPartitions MessageType = 6
	messageTypeCreateTopic        MessageType = 7
	messageTypeReloadConf         MessageType = 8
	messageTypeConsumePeer        MessageType = 9
	messageTypeStatus             MessageType = 10
)
