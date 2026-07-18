package main

import (
	server "Gredis/server"
	"fmt"
	"os"
	"os/signal"
)

// entry point for Gredis server
func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	//create new Gredis/server instance
	Gredis := server.NewGredis(":6379")

	Gredis.CreateChannel("welcome")

	//start Gredis server
	go Gredis.Serve()

	<-c
	fmt.Println("\nThank you for using Gredis")
}
