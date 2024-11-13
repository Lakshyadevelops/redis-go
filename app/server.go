package main

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var _m = make(map[string]string)

func ping() ([]byte, error) {
	resp := "+PONG\r\n"
	return []byte(resp), nil
}

func echo(input []string) ([]byte, error) {
	text := "+" + input[1][:] + "\r\n"
	return []byte(text), nil
}

func expire(key string, ms int) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
	delete(_m, key)
}

func set(input []string) ([]byte, error) {
	if len(input) != 4 && len(input) != 8 {
		return nil, errors.ErrUnsupported
	}

	px := 0
	if len(input) == 8 {
		if strings.ToUpper(input[5][:]) != "PX" {
			return nil, errors.ErrUnsupported
		}
		var err error
		px, err = strconv.Atoi(input[7][:])
		if err != nil {
			return nil, errors.ErrUnsupported
		}
		if px == 0 {
			return nil, errors.ErrUnsupported
		}
	}

	key := input[1][:]
	value := input[3][:]
	_m[key] = value

	if px != 0 {
		go expire(key, px)
	}
	return []byte("+OK\r\n"), nil
}

func get(input []string) ([]byte, error) {
	if len(input) < 2 {
		return nil, errors.ErrUnsupported
	}
	key := input[1][:]
	value, ok := _m[key]
	if ok {
		resp := fmt.Sprintf("$%v\r\n%v\r\n", len(value), value)
		fmt.Println(resp)
		return []byte(resp), nil
	}
	return []byte("$-1\r\n"), nil

}

func respParser(data []byte) ([]byte, error) {

	str := string(data)
	str = strings.TrimSpace(str)
	input_array := strings.Split(str, "\r\n")
	// Remove Trailing Empty
	if len(input_array) > 0 {
		input_array = input_array[:len(input_array)-1]
	}

	cmd := strings.ToUpper(input_array[2*1][:])
	switch cmd {
	case "ECHO":
		return echo(input_array[3:])
	case "PING":
		return ping()
	case "SET":
		return set(input_array[3:])
	case "GET":
		return get(input_array[3:])
	default:
		return nil, errors.ErrUnsupported
	}
}

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	defer l.Close()

	// EVENT LOOP
	for {
		con, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go func(con net.Conn) {
			defer con.Close()

			data := make([]byte, 256)
			for {
				_, err := con.Read(data)
				if errors.Is(err, io.EOF) {
					return
				}
				resp, err := respParser(data)
				if err != nil {
					fmt.Println(err)
					os.Exit(1)
				}
				con.Write([]byte(resp))
			}
		}(con)
	}
}
