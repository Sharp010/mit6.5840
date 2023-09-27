package main

import (
	"fmt"
	"os"
	"sync"
	"time"
)

type Kv struct {
	Key   string
	Value string
}

func main() {
	//oname := "test-json"
	//ofile, err := os.Create(oname)
	//if err != nil {
	//	fmt.Printf("open error!")
	//}
	//defer ofile.Close()
	//enc := json.NewEncoder(ofile)
	//kvs := []Kv{{"one", "1"}, {"two", "2"}, {"three", "3"}}
	//for _, kv := range kvs {
	//	err := enc.Encode(&kv)
	//	if err != nil {
	//		fmt.Printf("encode error!")
	//	}
	//}

	//ofile, err := os.Open(oname)
	//if err != nil {
	//	fmt.Printf("open file error!")
	//}
	//defer ofile.Close()
	//dec := json.NewDecoder(ofile)
	//var kvs []Kv
	//for {
	//	var kvi Kv
	//	if err := dec.Decode(&kvi); err != nil {
	//		break
	//	}
	//	kvs = append(kvs, kvi)
	//}
	//for _, v := range kvs {
	//	fmt.Printf("key:%s  value:%s\n", v.Key, v.Value)
	//}
	//
	//files, err := filepath.Glob("./mr-2-*")
	//if err != nil {w
	//	fmt.Println(err)
	//}
	//for _, file := range files {
	//	fmt.Println(file)
	//}

	//filname := "hello"
	//err := os.Rename(filname, "goodbye")
	//if err != nil {
	//	fmt.Printf("rename error!")
	//} else {
	//	fmt.Printf("rename success!")
	//}

	//kvs := make(map[int][]mr.KeyValue)
	//kvs[1] = append(kvs[1], mr.KeyValue{
	//	Key:   "1",
	//	Value: "2",
	//})
	//fmt.Printf("%v\n", kvs[1][0])
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		ofile, err := os.Open("test.txt")
		if err != nil {
			fmt.Printf("go [1] open failed!\n")
		}
		fmt.Printf("go [1] operating!\n")
		time.Sleep(5 * time.Second)
		ofile.Close()
		fmt.Printf("go [1] done!\n")
		wg.Done()
	}()
	go func() {
		ofile, err := os.Open("test.txt")
		if err != nil {
			fmt.Printf("go [2] open failed!\n")
		}
		fmt.Printf("go [2] operating!\n")
		time.Sleep(5 * time.Second)
		ofile.Close()
		fmt.Printf("go [2] done!\n")
		wg.Done()
	}()
	wg.Wait()
}
