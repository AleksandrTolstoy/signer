package main

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
)

const workerCount = 7

func Md5(data string, out chan string, m sync.Locker) {
	m.Lock()
	out <- DataSignerMd5(data)
	m.Unlock()
}

func Crc32(data string, out chan string) {
	out <- DataSignerCrc32(data)
}

func SingleHash(in, out chan interface{}) {
	m := new(sync.Mutex)
	wg := new(sync.WaitGroup)

	for inputData := range in {
		wg.Add(1)
		go func(inputData int) {
			defer wg.Done()

			data := strconv.Itoa(inputData)

			resAChan := make(chan string, 1)
			resBChan := make(chan string, 1)
			resCChan := make(chan string, 1)

			go Crc32(data, resAChan)
			go Md5(data, resBChan, m)
			go Crc32(<-resBChan, resCChan)

			result := <-resAChan + "~" + <-resCChan
			fmt.Printf("%s SingleHash result: %s\n", data, result)
			out <- result
		}(inputData.(int))
	}
	wg.Wait()
}

func MultiHash(in, out chan interface{}) {
	thCount := 6
	wg := new(sync.WaitGroup)

	for singleHash := range in {
		wg.Add(1)
		go func(singleHash string) {
			defer wg.Done()

			hashParts := make(map[int]chan string)
			for th := 0; th < thCount; th++ {
				hashChan := make(chan string, 1)
				hashParts[th] = hashChan
				go Crc32(strconv.Itoa(th)+singleHash, hashChan)
			}

			result := ""
			for i := 0; i < thCount; i++ {
				result += <-hashParts[i]
			}
			fmt.Printf("%s MultiHash result: %s\n", singleHash, result)
			out <- result
		}(singleHash.(string))
	}
	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	idx := 0
	multiHashes := make([]string, workerCount)
	for multiHash := range in {
		multiHashes[idx] = multiHash.(string)
		idx++
	}
	sort.Strings(multiHashes)

	result := ""
	for _, multiHash := range multiHashes {
		result += multiHash + "_"
	}
	result = result[:len(result)-1]
	fmt.Printf("CombineResults: %s\n", result)
	out <- result
}

func ExecutePipeline(hashSignJobs ...job) {
	in := make(chan interface{}, workerCount)
	wg := new(sync.WaitGroup)

	for _, j := range hashSignJobs {
		wg.Add(1)
		out := make(chan interface{}, workerCount)
		go func(job job, in, out chan interface{}) {
			defer wg.Done()
			defer close(out)
			job(in, out)
		}(j, in, out)
		in = out
	}
	wg.Wait()
}
