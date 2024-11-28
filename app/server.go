package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/wk8/go-ordered-map"
	"io"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var _m = make(map[string]string)
var _x = make(map[string]*orderedmap.OrderedMap)
var _info info

type info struct {
	Role               role
	Master_replid      string
	Master_repl_offset int64
}

type role string

const (
	MASTER role = "master"
	SLAVE  role = "slave"
)

func ping_cmd() ([]byte, error) {
	resp := "+PONG\r\n"
	return []byte(resp), nil
}

func echo_cmd(input []string) ([]byte, error) {
	text := "+" + input[1][:] + "\r\n"
	return []byte(text), nil
}

func expire(key string, ms int64) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
	delete(_m, key)
}

func set_cmd(input []string) ([]byte, error) {
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

func get_cmd(input []string) ([]byte, error) {
	if len(input) < 2 {
		return nil, errors.ErrUnsupported
	}
	key := input[1][:]
	cmdtype, _ := typecmd(input)
	switch string(cmdtype[1 : len(cmdtype)-2]) {
	case "string":
		value := _m[key]
		resp := fmt.Sprintf("$%v\r\n%v\r\n", len(value), value)
		return []byte(resp), nil
	case "stream":
		return nil, errors.ErrUnsupported
	default:
		return []byte("$-1\r\n"), nil
	}
}

func generate_sequence_xadd(timestamp int64, key string) int64 {
	_, ok := _x[key]
	if !ok {
		_x[key] = orderedmap.New()
		if timestamp == 0 {
			return 1
		}
		return 0
	}
	om := _x[key]
	keyID := om.Newest().Key
	arr := strings.Split(keyID.(string), "-")
	timestamp_old, _ := strconv.ParseInt(arr[0], 10, 64)
	if timestamp_old == timestamp {
		seq, _ := strconv.ParseInt(arr[1], 10, 64)
		seq++
		return seq
	}
	return 0
}

func xadd_cmd(input []string) ([]byte, error) {
	key := input[1][:]
	timestamp_new := int64(0)
	sequence_new := int64(0)

	keyID := input[3][:]
	if keyID == "*" {
		timestamp_new = time.Now().UnixMilli()
		sequence_new = generate_sequence_xadd(timestamp_new, key)
		keyID = strconv.FormatInt(timestamp_new, 10) + "-" + strconv.FormatInt(sequence_new, 10)
	} else if keyID[len(keyID)-2:] == "-*" {
		timestamp_new, _ = strconv.ParseInt(keyID[:len(keyID)-2], 10, 64)
		sequence_new = generate_sequence_xadd(timestamp_new, key)
		keyID = strconv.FormatInt(timestamp_new, 10) + "-" + strconv.FormatInt(sequence_new, 10)
	} else {
		array_new := strings.Split(keyID, "-")
		timestamp_new, _ = strconv.ParseInt(array_new[0], 10, 64)
		sequence_new, _ = strconv.ParseInt(array_new[1], 10, 64)
	}

	if timestamp_new < 0 || sequence_new < 0 || (timestamp_new == 0 && sequence_new == 0) {
		return []byte("-ERR The ID specified in XADD must be greater than 0-0\r\n"), nil
	}

	value := make([]string, 0, 10)
	for i := 5; i < len(input); i += 2 {
		value = append(value, input[i])
	}
	serialized, err := json.Marshal(value)
	if err != nil {
		return nil, errors.New("error serializing")
	}
	_, ok := _x[key]
	if !ok {
		_x[key] = orderedmap.New()
	}
	om := _x[key]
	pair := om.Newest()

	timestamp_old := int64(0)
	sequence_old := int64(0)
	if pair != nil {
		array_old := strings.Split(pair.Key.(string), "-")
		timestamp_old, err = strconv.ParseInt(array_old[0], 10, 64)
		sequence_old, err = strconv.ParseInt(array_old[1], 10, 64)
	}
	if timestamp_new < timestamp_old || (timestamp_new == timestamp_old && sequence_new <= sequence_old) {
		return []byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"), nil
	}

	_x[key].Set(keyID, serialized)
	resp := fmt.Sprintf("$%v\r\n%v\r\n", len(keyID), keyID)
	return []byte(resp), nil
}

func config_cmd(input []string) ([]byte, error) {
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

func keys_cmd(input []string) ([]byte, error) {
	// TODO : SUPPORT KEY FILTERING
	arr := make([]string, 0, 10)
	for key := range _m {
		arr = append(arr, key)
	}
	return []byte(encodeRESPArray(arr)), nil
}

func typecmd(input []string) ([]byte, error) {
	key_type := type_impl(input)
	switch key_type {
	case "STRING":
		return []byte("+string\r\n"), nil
	case "STREAM":
		return []byte("+stream\r\n"), nil
	default:
		return []byte("+none\r\n"), nil
	}
}

func type_impl(input []string) string {
	key := input[1][:]
	if _, ok := _m[key]; ok {
		return "STRING"
	}
	if _, ok := _x[key]; ok {
		return "STREAM"
	}
	return "NONE"
}

func info_cmd(input []string) ([]byte, error) {
	// TODO : implement input parsing and returning only relevant fields

	val := reflect.ValueOf(_info)
	typ := val.Type()
	infos := make([]string, 0, 10)

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)
		infos = append(infos, fmt.Sprintf("%s:%v", strings.ToLower(fieldType.Name), field.Interface()))
	}
	item := strings.Join(infos, " ")
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(item), item)), nil
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

func replconf_cmd(input []string) ([]byte, error) {
	return []byte("+OK\r\n"), nil
}

func psync_cmd(input []string) ([]byte, error) {
	// TO DO : Allow Sending Any RDB

	// SAMPLE RDB
	rdb := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	decodedBytes, _ := hex.DecodeString(rdb)
	resp := make([]byte, 0, 512)
	resp = append(resp, []byte(fmt.Sprintf("+FULLRESYNC %v %v\r\n$%v\r\n", _info.Master_replid, _info.Master_repl_offset, len(decodedBytes)))...)
	resp = append(resp, decodedBytes...)
	fmt.Println(resp)
	return resp, nil
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
		return echo_cmd(input_array[3:])
	case "PING":
		return ping_cmd()
	case "SET":
		return set_cmd(input_array[3:])
	case "GET":
		return get_cmd(input_array[3:])
	case "CONFIG":
		return config_cmd(input_array[3:])
	case "KEYS":
		return keys_cmd(input_array[3:])
	case "TYPE":
		return typecmd(input_array[3:])
	case "XADD":
		return xadd_cmd(input_array[3:])
	case "INFO":
		return info_cmd(input_array[3:])
	case "REPLCONF":
		return replconf_cmd(input_array[3:])
	case "PSYNC":
		return psync_cmd(input_array[3:])
	default:
		return nil, errors.ErrUnsupported
	}
}

var dir, dbfilename, replicaof string
var port int

func main() {
	flag.StringVar(&dir, "dir", "/tmp/redis-data", "Directory to store RDB snapshot")
	flag.StringVar(&dbfilename, "dbfilename", "rdbfile", "RDB Filename")
	flag.IntVar(&port, "port", 6379, "port to run server")
	flag.StringVar(&replicaof, "replicaof", "", "<MASTER_HOST> <MASTER_PORT>")

	flag.Parse()
	if replicaof != "" {
		_info.Role = SLAVE
	} else {
		_info.Role = MASTER
	}
	// TODO : Master_replid generation
	_info.Master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

	fmt.Println("Logs from your program will appear here!")

	err := loadRDB(dir, dbfilename)
	if err != nil {
		fmt.Println("Error Loading RDB File")
	} else {
		fmt.Println("RDB File loaded succesfully")
	}

	if _info.Role == SLAVE {
		arr := strings.Split(replicaof, " ")
		master_host := arr[0]
		master_port := arr[1]
		conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", master_host, master_port))
		if err != nil {
			fmt.Println("Error connecting to Master")
			os.Exit(1)
		}
		defer conn.Close()

		s := "*1\r\n$4\r\nPING\r\n"
		conn.Write([]byte(s))
		sa := []string{"REPLCONF", "listening-port", strconv.Itoa(port)}
		buffer := make([]byte, 512)
		conn.Read(buffer)

		conn.Write([]byte(encodeRESPArray(sa)))
		conn.Read(buffer)

		sa = []string{"REPLCONF", "capa", "psync2"}
		conn.Write([]byte(encodeRESPArray(sa)))
		conn.Read(buffer)

		sa = []string{"PSYNC", "?", "-1"}
		conn.Write([]byte(encodeRESPArray(sa)))
		conn.Read(buffer)
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%v", port))
	if err != nil {
		fmt.Printf("Failed to bind to port %v", port)
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
