package Server

import (
	client "Gredis/Client"
	"Gredis/Server/db"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

type Gredis struct {
	userCount   int
	connections map[int]*client.Client
	port        string
	addres      string
	db          *db.Db
	mu          sync.Mutex
}

func NewGredis(port string) *Gredis {
	return &Gredis{connections: make(map[int]*client.Client), port: port, db: db.NewGredisDb()}
}

// start Gredis server
func (s *Gredis) Serve() {

	ln, err := net.Listen("tcp", s.port)

	s.restore()

	if err != nil {
		fmt.Printf("listner not set due to erro: %v", err)
	}

	//print ascii for server
	s.printASCII()

	//start aof worker
	go s.db.StartAofWorker()

	for {
		//await connection
		conn, err := ln.Accept()

		if err != nil {
			fmt.Printf("connection not possible %v", err)
		}

		//handle connection concurrently
		go s.handleConnection(conn)
	}

}

// handle individual connection
func (s *Gredis) handleConnection(c net.Conn) {
	c.Write([]byte("Connection established user\n"))

	//buffer
	buffer := make([]byte, 4000)

	for {
		//read data into buffer
		n, err := c.Read(buffer)
		if err != nil {
			break
		}

		// go parseCommand(buffer[:n], s.db)

		protocol, err := parseCommand(buffer[:n], s.db)

		if err != nil {
			c.Write([]byte(err.Error()))
		}

		c.Write([]byte(protocol + "\n"))
	}

}

func parseCommand(b []byte, db *db.Db) (string, error) {
	protocol := strings.ToUpper(string(b))

	parts := strings.SplitN(protocol, " ", 3)

	if len(parts) < 2 {
		return "", errors.New("Invalid command try [GET KEY | SET KEY VALUE]")
	}

	action := parts[0]
	key := strings.TrimSuffix(parts[1], "\n")

	switch action {
	case "GET":
		//GET KEY
		value, err := db.Get(key)
		if err != nil {
			return "", err
		}

		return value, nil
	case "SET":
		//write command to worker for backup
		db.CmdCh <- protocol
		value := strings.TrimSuffix(parts[2], "\n")
		//SET KEY
		db.Set(key, value)
		return "SET command works hi form gredis", nil
	default:
		return "Choose command SET OR GET", nil
	}
}

func (s *Gredis) printASCII() {
	fmt.Printf(`  _________                                               .__  .__             ._.
 /   _____/ ______________  __ ___________    ____   ____ |  | |__| ____   ____| |
 \_____  \_/ __ \_  __ \  \/ // __ \_  __ \  /  _ \ /    \|  | |  |/    \_/ __ \ |
 /        \  ___/|  | \/\   /\  ___/|  | \/ (  <_> )   |  \  |_|  |   |  \  ___/\|
/_______  /\___  >__|    \_/  \___  >__|     \____/|___|  /____/__|___|  /\___  >_
        \/     \/                 \/                    \/             \/     \/\/
          __________           _________                                          
          \______   \___.__.  /   _____/____    _____                             
           |    |  _<   |  |  \_____  \\__  \  /     \                            
           |    |   \\___  |  /        \/ __ \|  Y Y  \                           
           |______  // ____| /_______  (____  /__|_|  /                           
                  \/ \/              \/     \/      \/                            
                                                                                  
                                                                                  
                                                                                  
                                                                                  
                                                                                  
                                                                                  `)
}

// read from aof file to restore data after crash or restart
func (s *Gredis) restore() {

	//buffer
	buff := make([]byte, 300)
	f, err := os.Open("aof.txt")

	if err != nil {
		fmt.Println("restoring data went wrong")
	}

	n, _ := f.Read(buff)

	fmt.Printf("%v", string(buff[:n]))

	start := 0
	for num, value := range buff[:n] {
		if value == '\n' {
			conn, _ := net.Dial("tcp", ":6379")
			conn.Write(buff[start : num+1])

			//get the enter for the send
			fmt.Println("test:", string(buff[start:num+1]))

			//get rid of enter for next iteration
			start = num + 1
		}
	}

}
