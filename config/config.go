package config

import (
	"leexsh/gee/myrpc/rpc/codec"
	"time"
)

const MagicNumber = 0x3bef5c

type Option struct {
	MagicNumber       int        // marks rpc request
	CodeType          codec.Type // client encode way
	ConnectionTimeout time.Duration
	HandleTimeout     time.Duration
}

var DefaultOption = &Option{
	MagicNumber:       MagicNumber,
	CodeType:          codec.GobType,
	ConnectionTimeout: time.Second * 20,
}
