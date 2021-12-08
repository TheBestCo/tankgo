package message

// MessageType alias.
type MessageType uint8

// Message types id.
const (
	TypeUnknown            MessageType = 0
	TypeConsume            MessageType = 2
	TypeProduce            MessageType = 1
	TypeProduce5           MessageType = 5
	TypePing               MessageType = 3
	TypeDiscoverPartitions MessageType = 6
	TypeCreateTopic        MessageType = 7
	TypeReloadConf         MessageType = 8
	TypeConsumePeer        MessageType = 9
	TypeStatus             MessageType = 10
)
