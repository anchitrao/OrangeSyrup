package main

import (
	"fmt"
	"sync"
	"strconv"
)

/*
 * StressTest
 *
 * Stress test the entire system!
 */
func StressTest(num_tests int) {
	StressTestPut(num_tests)
	StressTestGet(num_tests)
	StressTestDelete(num_tests)
}

/*
 * StressTestGet
 *
 * Do a bunch of get operations and try to fail them
 */
func StressTestGet(num_tests int) {
	fmt.Println("**** Testing Get ****")

	var wg sync.WaitGroup
	responses := make(chan error)

	// Launch a bunch of stress threads
	for i := 0 ; i < num_tests ; i++ {
		wg.Add(1)

		go func (i_in int) {
			defer wg.Done()
			responses <- StressGetThread(i_in)
		}(i)
	}

	// Wait for all goroutines to synchronize
	go func () {
		wg.Wait()
		close(responses)
	}()

	// See which ones failed
	num_failed := 0
	for resp := range responses {
		if resp != nil {
			num_failed += 1
			fmt.Println(resp)
		}
	}

	fmt.Println("Of", num_tests, "tests,", num_failed, "failed")
}

/*
 * StressGetThread
 *
 * An individual thread that stress tests get
 */
func StressGetThread(i int) error {
	return Get(strconv.Itoa(i), "files/gettest" + strconv.Itoa(i))
}

/*
 * StressTestDelete
 *
 * Do a bunch of delete operations and try to fail them
 */
func StressTestDelete(num_tests int) {
	fmt.Println("**** Testing Delete ****")

	var wg sync.WaitGroup
	responses := make(chan error)

	// Launch a bunch of stress threads
	for i := 0 ; i < num_tests ; i++ {
		wg.Add(1)

		go func (i_in int) {
			defer wg.Done()
			responses <- StressDeleteThread(i_in)
		}(i)
	}

	// Wait for all goroutines to synchronize
	go func () {
		wg.Wait()
		close(responses)
	}()

	// See which ones failed
	num_failed := 0
	for resp := range responses {
		if resp != nil {
			num_failed += 1
			fmt.Println(resp)
		}
	}

	fmt.Println("Of", num_tests, "tests,", num_failed, "failed")
}

/*
 * StressDeleteThread
 *
 * An individual thread that stress tests delete
 */
func StressDeleteThread(i int) error {
	return Delete(strconv.Itoa(i))
}

/*
 * StressTestPut
 *
 * Do a bunch of put operations and try to fail them
 */
func StressTestPut(num_tests int) {
	fmt.Println("**** Testing Put ****")

	var wg sync.WaitGroup
	responses := make(chan error)

	// Launch a bunch of stress threads
	for i := 0 ; i < num_tests ; i++ {
		wg.Add(1)

		go func (i_in int) {
			defer wg.Done()
			responses <- StressPutThread(i_in)
		}(i)
	}

	// Wait for all goroutines to synchronize
	go func () {
		wg.Wait()
		close(responses)
	}()

	// See which ones failed
	num_failed := 0
	for resp := range responses {
		if resp != nil {
			num_failed += 1
			fmt.Println(resp)
		}
	}

	fmt.Println("Of", num_tests, "tests,", num_failed, "failed")
}

/*
 * StressPutThread
 *
 * An individual thread that stress tests put
 */
func StressPutThread(i int) error {
	return Put("main.go", strconv.Itoa(i))
}
