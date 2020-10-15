package main

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

var (
	totalSingleHashes uint32 = 0
	totalMultiHashes  uint32 = 0
	workerCount       uint32 = 7 // len(inputData)
	thCount                  = 6
)

func inc(counter *uint32) {
	atomic.AddUint32(counter, 1)
}

func Md5(data string, out chan string, m *sync.Mutex) {
	m.Lock()
	out <- DataSignerMd5(data)
	m.Unlock()
}

func Crc32(data string, out chan string) {
	out <- DataSignerCrc32(data)
}

type indexedCrc32 struct {
	index int
	value string
}

func Crc32WithIndexing(th int, data string, out chan *indexedCrc32, wg *sync.WaitGroup) {
	defer wg.Done()

	indexedCrc32 := &indexedCrc32{
		index: th,
		value: DataSignerCrc32(strconv.Itoa(th) + data),
	}
	out <- indexedCrc32
}

func SingleHash(in, out chan interface{}) {
	m := &sync.Mutex{}

	for i := 0; i < int(workerCount); i++ {
		go func() {
			data := strconv.Itoa((<-in).(int))

			resAChan := make(chan string)
			resBChan := make(chan string)
			resCChan := make(chan string)

			go Crc32(data, resAChan)
			go Md5(data, resBChan, m)
			go Crc32(<-resBChan, resCChan)

			result := <-resAChan + "~" + <-resCChan
			fmt.Printf("%s SingleHash result: %s\n", data, result)
			out <- result
			if inc(&totalSingleHashes); totalSingleHashes == workerCount {
				close(out)
			}
		}()
	}
}

func MultiHash(in, out chan interface{}) {
	for singleHash := range in {
		go func(singleHash string) {
			hashChan := make(chan *indexedCrc32, thCount)
			wg := &sync.WaitGroup{}
			for i := 0; i < thCount; i++ {
				wg.Add(1)
				go Crc32WithIndexing(i, singleHash, hashChan, wg)
			}
			wg.Wait()
			close(hashChan)

			hashParts := make(map[int]string)
			for hashPart := range hashChan {
				hashParts[hashPart.index] = hashPart.value
			}

			var result string
			for i := 0; i < thCount; i++ {
				result += hashParts[i]
			}

			fmt.Printf("%s MultiHash result: %s\n", singleHash, result)
			out <- result
			if inc(&totalMultiHashes); totalMultiHashes == workerCount {
				close(out)
			}
		}(singleHash.(string))
	}
}

func CombineResults(in, out chan interface{}) {
	multiHashes := make([]string, 0)
	for multiHash := range in {
		multiHashes = append(multiHashes, multiHash.(string))
	}
	sort.Strings(multiHashes)

	result := ""
	for _, v := range multiHashes {
		result += v + "_"
	}

	result = result[:len(result) - 1]
	fmt.Printf("CombineResults: %s\n", result)
	out <- result
	close(out)
}

func ExecutePipeline(hashSignJobs... job){
	in := make(chan interface{}, workerCount)
	for _, job := range hashSignJobs {
		out := make(chan interface{}, workerCount)
		job(in, out)
		in = out
	}
}
