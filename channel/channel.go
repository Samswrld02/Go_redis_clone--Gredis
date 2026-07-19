package channel

import (
	client "Gredis/client"
	"fmt"
	"sync"
)

type Channel struct {
	Name       string
	Subscribed map[int]*client.Client
	Ch         chan string
	mu         sync.Mutex
}

// create channel
func NewChannel(name string) *Channel {
	//start channel worker
	channel := &Channel{Name: name, Subscribed: make(map[int]*client.Client, 0), Ch: make(chan string, 100)}

	respCh := channel.BroadcastWorker()

	//check response/ background process, blocking operation
	go func() {
		for resp := range respCh {
			if resp.Err != nil {
				//remove client from subscribed map
				channel.removeSubscribed(resp.Client)
				//timeout connection
				resp.Client.Connection.Close()
			}
		}
	}()

	return channel
}

// response struct
type Response struct {
	Resp   bool
	Err    error
	Client *client.Client
}

func (c *Channel) removeSubscribed(client *client.Client) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.Subscribed, client.Id)
}

// broadcast worker in the background, listens to channel continuesly and pushes data to respective clients
func (c *Channel) BroadcastWorker() <-chan *Response {
	resp := make(chan *Response)

	//push each message in each client's channel that's subscribed
	go func() {
		for mess := range c.Ch {
			for _, client := range c.Subscribed {
				select {
				case client.Mess <- mess:
					fmt.Println("message send to user")
					resp <- &Response{
						Resp:   true,
						Err:    nil,
						Client: client,
					}
				default:
					//clients message queue is full, disconnect/timeout
					resp <- &Response{
						Resp:   false,
						Err:    fmt.Errorf("queue is full, disconnect"),
						Client: client,
					}
				}
				// client.Connection.Write([]byte(mess))
				fmt.Println("message added to client")
			}
		}
	}()
	return resp
}

// subscribe to channel
func (c *Channel) Subscribe(client *client.Client) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	//register client to channel
	c.Subscribed[client.Id] = client

	return true, nil
}
