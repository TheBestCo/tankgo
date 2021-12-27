package message

// Type alias.
type Type uint8

// Message types id.
const (
	TypeUnknown            Type = 0
	TypeConsume            Type = 2
	TypeProduce            Type = 1
	TypeProduce5           Type = 5
	TypePing               Type = 3
	TypeDiscoverPartitions Type = 6
	TypeCreateTopic        Type = 7
	TypeReloadConf         Type = 8
	TypeConsumePeer        Type = 9
	TypeStatus             Type = 10
)
