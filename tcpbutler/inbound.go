package tcpbutler

import "context"

type InboundHandler func(ctx context.Context, message Message) (resp Message, err error)
