package main

import (
	"6.5840/shardctrler"
	"bytes"
	"compress/gzip"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
)

type Kv struct {
	Key   string
	Value string
}
type Entry struct {
	Term    int
	Command interface{}
}
type Ans struct {
	Seq   int
	Reply string
}

func CompressJson(logs []Entry) ([]byte, error) {
	//data, err := json.Marshal(logs)
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(logs)
	if err != nil {
		log.Fatal(err)
	}
	data := buf.Bytes()
	if err != nil {
		return nil, err
	}
	var zipbuf bytes.Buffer
	gz := gzip.NewWriter(&zipbuf)
	if _, err := gz.Write(data); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return zipbuf.Bytes(), nil
}
func DecompressJSON(data []byte) ([]Entry, error) {
	var logs []Entry
	buf := bytes.NewBuffer(data)
	gz, err := gzip.NewReader(buf)
	if err != nil {
		return logs, err
	}
	defer gz.Close()
	compressedData, err := ioutil.ReadAll(gz)
	if err != nil {
		return logs, err
	}
	//err = json.Unmarshal(compressedData, &logs)
	dec := gob.NewDecoder(bytes.NewReader(compressedData))
	err = dec.Decode(&logs)
	//json
	if err != nil {
		return logs, err
	}
	return logs, nil
}

type kvs struct {
	keys   string
	values string
}

func (kv *kvs) pt3(s string) {
	fmt.Printf(s)
}

func pt() {
	fmt.Printf("hello\n")
}
func pt2() {
	fmt.Printf(" world!\n")
}
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}
func main() {
	strs := [...]string{"Downtown", "Perryridge", "Brighton", "Mianus", "Redwood", "Round Hill"}
	strLen := len(strs)
	for i := 300001; i <= 600000; i++ {
		fmt.Printf("INSERT INTO ACCOUNT109 VALUE "+"('A-%d','%s',%d);\n", i, strs[rand.Int()%strLen], (rand.Int()%100+30)*10)
	}

}
