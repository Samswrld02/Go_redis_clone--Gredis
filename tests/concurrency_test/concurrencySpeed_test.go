package concurrency_test

import (
	"net"
	"sync"
	"testing"
)

func TestConcurrency(t *testing.T) {
	//make channel
	request := 10000
	errChan := make(chan error, request)

	port := ":6379"
	network := "tcp"

	//make waitgroup to wait for all routines to finish
	var wg sync.WaitGroup

	//create loop for concurrent requests
	for i := 0; i < request; i++ {
		wg.Add(1)
		//run anonymous function concurrently
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			conn, err := net.Dial(network, port)

			if err != nil {
				errChan <- err
				return
			}

			defer conn.Close()

			_, err = conn.Write([]byte("SET SAM 1\n"))
			if err != nil {
				errChan <- err
				return
			}

		}(&wg)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			t.Errorf("concurrent request failed %v", err)
			break
		}
	}
}
