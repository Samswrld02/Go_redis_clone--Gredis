package channel

import (
	client "Gredis/Client"
	"fmt"
)

type Channel struct {
	Name       string
	Subscribed []*client.Client
	Ch         chan string
}

// create channel
func NewChannel(name string) *Channel {
	return &Channel{Name: name, Subscribed: make([]*client.Client, 0), Ch: make(chan string, 100)}
}

// broadcast worker in the background, listens to channel continuesly and pushes data to respective clients
func (c *Channel) BroadcastWorker() {
	//push each message in each client's channel that's subscribed
	go func() {
		for mess := range c.Ch {
			for _, client := range c.Subscribed {
				select {
				case client.Mess <- mess:
				default:
				}

			}
			fmt.Println("channel is blocked")
		}
	}()
}
