package client

import (
	"bufio"
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
	return &Client{Id: ai.ID(), Connection: c, Mess: make(chan string, 1000)}
}

func (c *Client) SendMessage() {
	for mess := range c.Mess {
		bufio.NewWriter(c.Connection).Write([]byte(mess))
	}
}
