package client

import (
	"net"
	"sync"
)

// intialise global struct for autoincrementing
var ai autoID

type autoID struct {
	mu sync.Mutex
	id int
}

// increments id automatically
func (a *autoID) ID() (id int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.id++
	return a.id
}

func NewId() *autoID {
	return &autoID{}
}

type Client struct {
	idCounter  int
	Id         int
	Connection net.Conn
	Mess       chan string
}

func NewClient(c net.Conn) *Client {
	client := &Client{Id: ai.ID(), Connection: c, Mess: make(chan string, 1000)}

	//start writer/broadcaster, indefinetly
	go client.writerWorker()

	return client
}

// infinitely listen to messages channel and broadcast
func (c *Client) writerWorker() {

	for value := range c.Mess {
		c.Connection.Write([]byte(value))
	}
}
