package concurrency_test

import (
	"fmt"
	"net"
	"sync"
	"testing"
)

func TestConcurrency(t *testing.T) {

	fmt.Println("Concurrency test busy..")

	//make channel
	request := 1
	errChan := make(chan error, request)

	port := ":6379"
	network := "tcp"

	//make waitgroup to wait for all routines to finish
	var wg sync.WaitGroup

	//make concurrent request
	for range request {

		wg.Add(1)

		go func(wg *sync.WaitGroup) {
			//connect to gredis
			conn, err := net.Dial(network, port)
			if err != nil {
				//write to errorchannel
				errChan <- fmt.Errorf("connection error %v", err)
				return
			}

			defer conn.Close()
			defer wg.Done()

			//send command
			_, err = conn.Write([]byte("get SAM 1\n"))

			if err != nil {
				errChan <- fmt.Errorf("something went wrong %v", err)
			}
		}(&wg)
	}

	//wait for threads to finish
	wg.Wait()

	//close channel to loop over it
	close(errChan)
	for err := range errChan {
		if err != nil {
			t.Errorf("concurrent request failed %v", err)
			break
		}
	}
}
