package tcpbutler

type Message interface {
	CorrelationID() string
	Encode() ([]byte, error)
}
