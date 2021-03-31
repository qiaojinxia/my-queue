package main

import "sync/atomic"

var id int64

func GetId() int64 { return atomic.AddInt64(&id, 1) }

type ServerOptions struct {
	address  string // listening address, e.g. :( ip://127.0.0.1:8080、 dns://www.google.com)
	network  string // network type, e.g. : tcp、udp
	protocol string // protocol typpe, e.g. : proto、json
}

type ServerOption func(*ServerOptions)

func WithAddress(address string) ServerOption {
	return func(o *ServerOptions) {
		o.address = address
	}
}

func WithNetwork(network string) ServerOption {
	return func(o *ServerOptions) {
		o.network = network
	}
}
