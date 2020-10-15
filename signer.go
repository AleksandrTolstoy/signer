package main

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
)

var (
	md5Mutex                = &sync.Mutex{}
	multiHashWaitGroup      = &sync.WaitGroup{}
	combineResultsWaitGroup = &sync.WaitGroup{}
)

func calcMd5(data string, out chan string) {
	md5Mutex.Lock()
	out <- DataSignerMd5(data)
	md5Mutex.Unlock()
}

func calcCrc32(data string, out chan string) {
	out <- DataSignerCrc32(data)
}

type indexedCrc32 struct {
	index int
	value string
}

func calcCrc32WithCounting(th int, data string, out chan *indexedCrc32) {
	defer multiHashWaitGroup.Done()

	indexedCrc32 := &indexedCrc32{
		index: th,
		value: DataSignerCrc32(strconv.Itoa(th) + data),
	}
	out <- indexedCrc32
}

func SingleHash(in, out chan interface{}) {
	data := strconv.Itoa((<-in).(int))

	resAChan := make(chan string, 1)
	resBChan := make(chan string, 1)
	resCChan := make(chan string, 1)

	go calcCrc32(data, resAChan)
	go calcMd5(data, resBChan)
	go calcCrc32(<-resBChan, resCChan)

	result := <-resAChan + "~" + <-resCChan
	fmt.Printf("%s SingleHash result: %s\n", data, result)
	out <- result
}

func MultiHash(in, out chan interface{}) {
	defer combineResultsWaitGroup.Done()
	data := (<-in).(string)

	var (
		count = 6
		hashChan = make(chan *indexedCrc32, count)
	)

	for th := 0; th < count; th++ {
		multiHashWaitGroup.Add(1)
		go calcCrc32WithCounting(th, data, hashChan)
	}
	multiHashWaitGroup.Wait()
	close(hashChan)

	hashParts := make(map[int]string)
	for hashPart := range hashChan {
		hashParts[hashPart.index] = hashPart.value
	}

	var result string
	for th := 0; th < count; th++ {
		result += hashParts[th]
	}

	fmt.Printf("%s MultiHash result: %s\n", data, result)
	out <- result
}

func CombineResults(in, out chan interface{}) {
	multiHashes := make([]string, 0)
	for v := range in {
		multiHashes = append(multiHashes, v.(string))
	}
	sort.Strings(multiHashes)

	result := ""
	for _, v := range multiHashes {
		result += v + "_"
	}
	result = result[:len(result) - 1]
	fmt.Printf("CombineResults: %s\n", result)
	out <- result
}

func ExecutePipeline(hashSignJobs... job){
	fillSingleHashIn := hashSignJobs[0]
	compareResults := hashSignJobs[4]

	workerCount := 7 // len(inputData)

	singleHashIn := make(chan interface{}, workerCount)
	fillSingleHashIn(make(chan interface{}), singleHashIn)

	singleHashOut := make(chan interface{}, workerCount)
	multiHashOut := make(chan interface{}, workerCount)
	combineResultsOut := make(chan interface{})
	for i := 0; i < workerCount; i++ {
		go SingleHash(singleHashIn, singleHashOut)
		combineResultsWaitGroup.Add(1)
		go MultiHash(singleHashOut, multiHashOut)
	}
	combineResultsWaitGroup.Wait()
	close(multiHashOut)

	go CombineResults(multiHashOut, combineResultsOut)
	compareResults(combineResultsOut, make(chan interface{}))
}
