package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	con, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer con.Close()

	data := make([]byte, 256)
	resp := "+PONG\r\n"
	for {
		_, err = con.Read(data)
		if errors.Is(err, io.EOF) {
			fmt.Println("Done")
			return
		}
		con.Write([]byte(resp))
		str := string(data)
		fmt.Println(str)
	}
}
