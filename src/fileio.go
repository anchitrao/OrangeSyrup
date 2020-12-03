package main

/*
 * fileio
 *
 * Methods for transferring files between nodes
 * and confirming their hashes.
 */

import (
	"fmt"
	"net"
	"os"
	"io"
	"encoding/json"
	"errors"
	"path"
	"strconv"
	"sync"
)

// Size of the file metadata buffer sent before a file
const FILE_METADATA_BUF_SIZE = 1024


// Only lock this AFTER locking my_files!
var hash_to_names = struct {
	sync.RWMutex

	// m maps SDFS filename hash -> SDFS name
	m map[string]string
}{m: make(map[string]string)}

/*************************
 *   File IO - Server    *
 *  Listen on Port 8080  *
 *************************/

/*
 * FileServer
 *
 * Accept TCP connections and download files upon connecting.
 */
func FileServer() error {
	// Receive raw TCP packets:
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println(err)
		return err
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("ERROR: ", err)
		}
		go ReceiveFile(conn)
	}

	return nil
}

/*
 * ReceiveFile
 *
 * Use bufio to receive a file using a TCP connection.
 */
func ReceiveFile(connection net.Conn) error {
	defer connection.Close()

	// Read file metadata as JSON
	var metadata_buf [FILE_METADATA_BUF_SIZE]byte
	bytes_read, err := connection.Read(metadata_buf[:])

	if bytes_read != FILE_METADATA_BUF_SIZE {
		return errors.New("Couldn't read file metadata!")
	}

	if err != nil {
		return err
	}

	// Calculate length of buffer
	buf_len := 0
	for idx, val := range metadata_buf {
		if val == '\x00' {
			buf_len = idx
			break
		}
	}

	metadata := make(map[string]string)
	err = json.Unmarshal(metadata_buf[0:buf_len], &metadata)

	if err != nil {
		fmt.Println(err)
		return err
	}

	// @TODO: Check if this file already exists!!!

	// Create files directory if missing
	_, err = os.Stat("files")
	if os.IsNotExist(err) {
		err_dir := os.MkdirAll("files", 0755)
		if err_dir != nil {
			fmt.Println("Couldn't create files directory")
			return err_dir
		}
	}

	file, err := os.Create(path.Join("files", metadata["name"]))

	if err != nil {
		fmt.Println("Couldn't create new file")
		return err
	}

	// Copy the rest of the stream into the file
	// The other node will close the TCP connection when the file is done sending.
	newfile_bytes_read, err := io.Copy(file, connection)
	if err != nil {
		return err
	}
	file.Close()

	// Ensure we read the correct number of bytes
	bytes_to_read, _ := strconv.ParseInt(metadata["size"], 10, 64)
	if newfile_bytes_read != bytes_to_read {
		fmt.Println("File size mismatch in receive file")
		// fmt.Println("Expected", string(bytes_to_read), "bytes")
		return errors.New("File size does not match expected value")
	}

	hash_to_names.Lock()
	defer hash_to_names.Unlock()

	hash_to_names.m[metadata["name"]] = metadata["realname"]

	return nil
}

/*************************
 *   File IO - Client    *
 *   Send on Port 8080   *
 *************************/

/*
 * SendFile
 *
 * Send a file to a server.
 *
 * server- IP of the server to send to
 * filename- the local file to send
 * filename_hash- the name (HASHED) to use for SDFS
 */
func SendFile(server string, filename string, filename_hash string, sdfsname string) error {
	if server == my_ip_addr {
		// We are trying to send to ourselves! Just copy the file over and call it, bypassing network code

		// @TODO: Refactor this + ReceiveFile into a single method that just
		// takes an io object and does all of this.

		// Create files directory if missing
		_, err := os.Stat("files")
		if os.IsNotExist(err) {
			err_dir := os.MkdirAll("files", 0755)
			if err_dir != nil {
				fmt.Println("Couldn't create files directory")
				return err_dir
			}
		}

		dst_file, err := os.Create(path.Join("files", filename_hash))
		if err != nil {
			fmt.Println("Couldn't create new file")
			return err
		}

		src_file, err := os.Open(filename)
		if err != nil {
			fmt.Println("Error opening file")
			fmt.Println(err)
			return err
		}

		// Copy source file directly to dest file
		_, err = io.Copy(dst_file, src_file)
		if err != nil {
			return err
		}
		dst_file.Close()
		src_file.Close()

		// Mark file as received
		hash_to_names.Lock()
		defer hash_to_names.Unlock()

		hash_to_names.m[filename_hash] = sdfsname

		// Done, no need for network stuff
		return nil
	}

	client, err := net.Dial("tcp", server + ":8080")
	if err != nil {
		fmt.Println(err)
		return err
	}

	defer client.Close()

	// Open file
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file!")
		fmt.Println(err)
		return err
	}
	defer file.Close()

	file_stat, err := file.Stat()
	if err != nil {
		fmt.Println("Error reading file stats")
		return err
	}

	file_size := file_stat.Size()

	// Send file metadata as JSON
	metadata := make(map[string]string)
	metadata["name"] = filename_hash
	metadata["size"] = fmt.Sprintf("%d", file_size)
	metadata["realname"] = sdfsname

	jsondata, err := json.Marshal(metadata)
	if err != nil {
		return err
	}

	if len(jsondata) > FILE_METADATA_BUF_SIZE {
		return errors.New(fmt.Sprintf("File metadata too big! (0x%x bytes)", len(jsondata)))
	}

	// Create a buffer of exactly FILE_METADATA_BUF_SIZE bytes and send it
	var metadata_buf [FILE_METADATA_BUF_SIZE]byte
	copy(metadata_buf[:], jsondata)

	client.Write(metadata_buf[:])

	bytes_written, err := io.Copy(client, file)

	if err != nil {
		fmt.Println("Error sending file!")
		fmt.Println(err)
		return err
	}

	if bytes_written != file_size {
		fmt.Println("Didn't write the same number of bytes as the file size! Did the connection drop?")
		fmt.Println("Sent", bytes_written, "expected to send", file_size)
		return errors.New("Didn't send enough data")
	}

	// Close TCP stream
	file.Close()
	client.Close()

	// Client should now ask for MD5 hash of received file
	// This check is unnecessary because TCP
	/*
	remote_hash, err := GetRemoteHash(server, filename_hash)
	if err != nil {
		fmt.Println(err)
		return err
	}

	local_hash, _ := GetFileHash(filename)

	if remote_hash != local_hash {
		return errors.New("File integrity failure- hash mismatch")
	}*/

	return nil
}
