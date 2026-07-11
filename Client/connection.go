package client

import (
	"net"
)

type Client struct {
	Id         int
	Connection *net.Conn
}

func NewClient(id int, c *net.Conn) *Client {
	return &Client{Id: id, Connection: c}
}
