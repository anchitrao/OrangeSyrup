package main

import (
	"fmt"
	"sync"
	"crypto/md5"
	"errors"
	"math/rand"
	"os"
	"path"
	"io"
	"time"
)

/*
 * Client
 *
 * Methods specific to client nodes
 *
 * This consists of code to interface with the master node, 
 * manage our tokens, and manage replicas of files we own.
 */

// Files we own and who else has a copy:
var my_files = struct {
	sync.RWMutex

	// m maps SDFS filename hash -> list of other node's IPs
	m map[string][]string
}{m: make(map[string][]string)}

/*
 * Put
 *
 * Put a file into SDFS!
 */
func Put(localname string, sdfsname string) error {
	filename_md5 := fmt.Sprintf("%x", md5.Sum([]byte(sdfsname)))
	master_start := master_id

	// Aquire write token to this file
	token, err := RequestToken(filename_md5, "W")

	if err != nil {
		return err
	}

	if token.Kind == "X" {
		return errors.New("Couldn't aquire write lock as the file is in use. Try again later.")
	}

	// Transfer the file to each replica server, ensuring the transfer was successful
	// Launch 4 SendFile goroutines and wait for their response
	// If any failed or had an error, we fail and releasetoken
	var wg sync.WaitGroup
	responses := make(chan error)

	num_transactions := 0
	for _, replica_ip := range token.Replicas {
		num_transactions += 1
		wg.Add(1)
		// fmt.Println("Sending to", replica_ip)

		go func (replica_ip_in string, localname_in string, filename_md5_in string, sdfsname_in string) {
			defer wg.Done()
			responses <- SendFile(replica_ip_in, localname_in, filename_md5_in, sdfsname_in)
		}(replica_ip, localname, filename_md5, sdfsname)
	}

	// Wait for all goroutines to synchronize
	go func () {
		wg.Wait()
		close(responses)
	}()

	// Fail if error on any transaction
	for resp := range responses {
		if resp != nil {
			fmt.Println("Error detected in SendFile")
			ReleaseToken(token)
			return resp
		}
	}

	// If we didn't send to anyone else, we fail
	if num_transactions == 0 {
		ReleaseToken(token)
		return errors.New("No other nodes to communicate with")
	}

	// If the master changed mid transfer, we fail
	if master_id != master_start {
		// Master has failed mid transfer!
		ReleaseToken(token)
		return errors.New("Put failed due to master failure mid transfer")
	}

	// Release token
	release_ok, err := ReleaseToken(token)
	if err != nil {
		fmt.Print("Couldn't release token: Error ")
		return err
	}
	if !release_ok {
		return errors.New("Couldn't release token! Did the master crash?")
	}

	// Tell each replica that the file is good to keep
	// Use FileIntegrity.ConfirmDownload RPC
	for _, replica_ip := range token.Replicas {
		// @TODO: goroutines here
		err = nil
		if replica_ip != my_ip_addr {
			err = ConfirmDownload(replica_ip, filename_md5, token.Replicas)
		} else {
			// Should be no different than confirming for remote servers, the
			// only issue with sending locally is with the bufio streams I think
			err = ConfirmDownload(replica_ip, filename_md5, token.Replicas)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

/*
 * Get
 *
 * Get a file from SDFS, if it exists!
 */
func Get(sdfsname string, localname string) error {
	filename_md5 := fmt.Sprintf("%x", md5.Sum([]byte(sdfsname)))
	master_start := master_id

	// Aquire read token to this file
	token, err := RequestToken(filename_md5, "R")

	if err != nil {
		return err
	}

	if token.Kind == "X" {
		return errors.New("Couldn't aquire lock, this file is in use. Try again later!")
	}

	// Read from a replica, ensuring the transfer was successful

	// Make sure the replica we Get from isn't us
	for idx, val := range token.Replicas {
		if val == my_ip_addr {
			fmt.Println("!! The server gave us our own replica !!")
			fmt.Println("This should never ever be able to happen")

			// Remove this replica from the replicas
			token.Replicas[idx] = token.Replicas[len(token.Replicas)-1]
			token.Replicas[len(token.Replicas)-1] = ""
			token.Replicas = token.Replicas[:len(token.Replicas)-1]
		}
	}

	if len(token.Replicas) == 0 {
		ReleaseToken(token)
		return errors.New("File not found.")
	}

	random_replica_idx := rand.Intn(len(token.Replicas))
	err = ReadRequest(token.Replicas[random_replica_idx], filename_md5)
	if err != nil {
		ReleaseToken(token)
		return err
	}

	// If the master changed mid transfer, we fail
	if master_id != master_start {
		// Master has failed mid transfer!
		ReleaseToken(token)
		return errors.New("Get failed due to master failure mid transfer")
	}

	// Release token
	release_ok, err := ReleaseToken(token)
	if err != nil {
		return err
	}
	if !release_ok {
		return errors.New("Couldn't release token! Did the master crash?")
	}

	// Grab file from files/hash and put it at sdfsfilename
	downloaded_file, err := os.Open(path.Join("files", filename_md5))
	if err != nil {
		fmt.Println("Error opening file", filename_md5)
		return err
	}
	defer downloaded_file.Close()

	// Write to file
	new_file, err := os.Create(localname)
	if err != nil {
		return err
	}
	defer new_file.Close()

	// Assume this succeeds
	io.Copy(new_file, downloaded_file)

	return nil
}

/*
 * Delete
 *
 * Remove a file from SDFS
 */
func Delete (sdfsname string) error {
	// time_start := time.Now()
	filename_md5 := fmt.Sprintf("%x", md5.Sum([]byte(sdfsname)))

	// Aquire write token to this file
	token, err := RequestToken(filename_md5, "W")

	if err != nil {
		return err
	}

	if token.Kind == "X" {
		return errors.New("Couldn't aquire write lock as the file is in use. Try again later.")
	}

	members.Lock()
	for _, member := range members.m {
		RemoveRequest(member.IP, filename_md5)
	}
	members.Unlock()

	ReleaseToken(token)

	return nil
}

/*
 * Append
 *
 * Append a string to a file in SDFS.
 *
 * sdfsname- the SDFS name of the file to append to
 * append_data- the string to place at the end
 */
func Append(sdfsname string, append_data []byte) error {
	// time_start := time.Now()
	filename_md5 := fmt.Sprintf("%x", md5.Sum([]byte(sdfsname)))

	// Aquire write token to this file
	var token TokenReply
	var err error

	token.Kind = "X"

	// Spin until we aquire the token
	spin_start := time.Now()
	for token.Kind == "X" {
		token, err = RequestToken(filename_md5, "W")

		if err != nil {
			ReleaseToken(token)
			return err
		}

		// Potentially time out
		if time.Since(spin_start).Seconds() > MAPLE_TIMEOUT {
			ReleaseToken(token)
			return errors.New("Timeout waiting for the write lock")
		}

		time.Sleep(time.Millisecond * 100)
	}

	for _, replica_ip := range token.Replicas {
		// fmt.Println("Sending append request to", replica_ip, "payload is", append_data)
		err = AppendRequest(replica_ip, filename_md5, sdfsname, append_data)

		if err != nil {
			ReleaseToken(token)
			fmt.Println("Error in Append")
			fmt.Println(err)
			return err
		}

		err = ConfirmDownload(replica_ip, filename_md5, token.Replicas)
		if err != nil {
			ReleaseToken(token)
			fmt.Println("Error confirming download in append")
			fmt.Println(err)
			return err
		}
	}

	ReleaseToken(token)

	return nil
}
