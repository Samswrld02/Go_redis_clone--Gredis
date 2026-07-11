package main

import (
	server "Gredis/Server"
)

// entry point for Gredis server
func main() {
	//create new Gredis/server instance
	Gredis := server.NewGredis(":6379")

	//start Gredis server
	Gredis.Serve()
}
