package main

import (
	"encoding/binary"
	"errors"
	"flag"
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

func expire(key string, ms int64) {
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
		go expire(key, int64(px))
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

func config(input []string) ([]byte, error) {
	switch input[1][:] {
	case "GET":
		return configGet(input[2:])
	// case "SET":
	// 	return configSet(input[2:])
	default:
		return nil, errors.ErrUnsupported
	}
}

func loadRDB(dir string, dbfilename string) error {

	path := ""
	if dir[len(dir)-1] == '/' {
		path = dir + dbfilename
	} else {
		path = dir + "/" + dbfilename
	}

	file, err := os.Open(path)
	if _, ok := err.(*os.PathError); ok {
		return os.ErrNotExist
	}
	defer file.Close()

	b := make([]byte, 1)
	// TODO : SUPPORT MULTIPLE DATABASES
	for {
		_, err := file.Read(b)
		if err != nil {
			if err.Error() == "EOF" {
				return errors.New("reached end of file without finding 0xFB")
			} else {
				return errors.New("error opening file")
			}
		}

		if b[0] == 0xFB {
			break
		}
	}

	file.Read(b)
	hash_table_size := int(b[0])
	file.Read(b)

	for i := 0; i < hash_table_size; i++ {
		// TODO : SUPPORT MULTIPLE KEY TYPES
		file.Read(b)
		expiry := false
		ms := int64(0)
		fmt.Println(b[0])
		if b[0] == 0xFC {
			expiry = true

			timestamp_buffer := make([]byte, 8)
			file.Read(timestamp_buffer)
			givenTime := binary.LittleEndian.Uint64(timestamp_buffer)
			currentTime := time.Now().UnixMilli()
			ms = int64(givenTime) - currentTime
		} else if b[0] == 0xfd {
			expiry = true

			timestamp_buffer := make([]byte, 4)
			file.Read(timestamp_buffer)
			givenTime := binary.LittleEndian.Uint64(timestamp_buffer)
			currentTime := time.Now().Unix()
			ms = int64(givenTime) - currentTime
		}
		if expiry {
			file.Read(b)
		}
		file.Read(b)
		key_buffer := make([]byte, int(b[0]))
		file.Read(key_buffer)
		file.Read(b)
		value_buffer := make([]byte, int(b[0]))
		file.Read(value_buffer)
		_m[string(key_buffer)] = string(value_buffer)
		if expiry {
			go expire(string(key_buffer), ms)
		}
	}
	return nil
}

func keys(input []string) ([]byte, error) {
	// TODO : SUPPORT KEY FILTERING
	arr := make([]string, 0, 10)
	for key := range _m {
		arr = append(arr, key)
	}
	return []byte(encodeRESPArray(arr)), nil
}

func configGet(input []string) ([]byte, error) {
	var resp []string
	switch input[1][:] {
	case "dir":
		resp = append(resp, "dir")
		resp = append(resp, dir)
		return []byte(encodeRESPArray(resp)), nil
	case "dbfilename":
		resp = append(resp, "dbfilename")
		resp = append(resp, dbfilename)
		return []byte(encodeRESPArray(resp)), nil
	default:
		return nil, errors.ErrUnsupported
	}
}

// func configSet(input []string) ([]byte, error) {

// }

func encodeRESPArray(data []string) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("*%d\r\n", len(data)))

	for _, item := range data {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item), item))
	}

	return sb.String()
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
	case "CONFIG":
		return config(input_array[3:])
	case "KEYS":
		return keys(input_array[3:])
	default:
		return nil, errors.ErrUnsupported
	}
}

var dir, dbfilename string

func main() {
	flag.StringVar(&dir, "dir", "/tmp/redis-data", "Directory to store RDB snapshot")
	flag.StringVar(&dbfilename, "dbfilename", "rdbfile", "RDB Filename")
	flag.Parse()

	fmt.Println("Logs from your program will appear here!")

	err := loadRDB(dir, dbfilename)
	if err != nil {
		fmt.Println("Error Loading RDB File")
	} else {
		fmt.Println("RDB File loaded succesfully")
	}

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
