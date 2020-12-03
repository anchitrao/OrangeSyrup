package main

import (
	"fmt"
	"os"
	"path"
	"net"
	"net/rpc"
	"errors"
	"strings"
)

/*
 * File Integrity Checks
 *
 * Used for checking file hashes, presence, etc. on a node.
 * Logically, master does not serve these RPCs, but nodes do.
 */

/*
 * Network synchronization object
 */
type FileIntegrity struct { }

/*
 * A hash integrity request, used by a node to confirm a file's hash.
 */
type FileIntegrityHashRequest struct {
	// Filename Hash
	Filename_Hash string `json:"filenamehash"`
}

/*
 * A has file request.
 */
type FileIntegrityHasFileRequest struct {
	// Filename Hash
	Filename_Hash string `json:"filenamehash"`
}

/*
 * A get all files request.
 */
type FileIntegrityGetAllFilesRequest struct {
	Prefix string `json:"prefix"`
}

/*
 * ConfirmDownload request
 */
type FileIntegrityConfirmRequest struct {
	// The SDFS filename hash
	Filename_Hash string `json:"filenamehash"`

	// A list of replica node IDs
	Replicas []string `json:"replicas"`
}

/*
 * ReadRequest request
 */
type FileIntegrityReadRequest struct {
	// The sender IP
	// (Hmm.. This could open us up to amplified DDOS attacks just like NTP :eyes:)
	Sender_IP string `json:"ip"`

	// The filehash to read from
	Filename_Hash string `json:"filenamehash"`
}

/*
 * RemoveRequest request
 */
type FileIntegrityRemoveRequest struct {
	// The filehash to delete
	Filename_Hash string `json:"filenamehash"`
}

/*
 * A request to append some data to a file
 */
type FileIntegrityAppendRequest struct {
	Filename_Hash string `json:"filenamehash"`
	Filename_Plain string `json:"filenameplain"`
	AppendData []byte `json:"appenddata"`
}

/*
 * A response from any integrity check
 */
type FileIntegrityResponse struct {
	// Whast whatever operation a success?
	Success bool `json:"success"`

	// Exists?
	File_Exists bool `json:"exists"`

	// Hash value
	Hash string `json:"hash"`

	// All files at this node matching prefix
	AllFiles []string `json:"all_files"`
}

/*
 * IntegrityRPCServer
 *
 * Launch file integrity RPC server
 */
func IntegrityRPCServer () error {
	networkState := new(FileIntegrity)

	// Setup TCP listener
	addr, err := net.ResolveTCPAddr("tcp", ":9090")

	if err != nil {
		fmt.Println("Error", err)
		return err
	}

	inbound, err := net.ListenTCP("tcp", addr)

	if err != nil {
		fmt.Println("Error", err)
		return err
	}

	rpc.Register(networkState)

	// Loop on accepting inputs
	rpc.Accept(inbound)

	return nil
}

/*
 * ConnectIntegrityRPC
 *
 * Returns a TCP connection to a remote integrity RPC listener
 */
func ConnectIntegrityRPC (server string) (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", server + ":9090")

	if err != nil {
		fmt.Print(server, " not responding (ConnectIntegrityRPC)\n")
	}

	return client, err
}

/******************
 *  RPC Wrappers  *
 ******************/

/*
 * GetRemoteHash
 *
 * Wrapper for calling ConfirmHash RPC
 */
func GetRemoteHash (server string, filename_hash string) (string, error) {
	connection, err := ConnectIntegrityRPC(server)
	if err != nil {
		return "", err
	}
	defer connection.Close()

	var reply FileIntegrityResponse
	reply.Success = false
	reply.File_Exists = false
	request := FileIntegrityHashRequest{filename_hash}
	err = connection.Call("FileIntegrity.GetHash", request, &reply)

	if !reply.File_Exists {
		return "", errors.New("File not found on remote server")
	}

	if !reply.Success {
		return "", errors.New("Error confirming hash")
	}

	if err != nil {
		return "", err
	}
	
	return reply.Hash, nil
}

/*
 * HasFile
 *
 * Wrapper for calling HasFile RPC
 */
func HasFile (server string, fileid string) bool {
	connection, err := ConnectIntegrityRPC(server)
	if err != nil {
		return false
	}
	defer connection.Close()

	var reply FileIntegrityResponse
	reply.Success = false
	reply.File_Exists = false
	request := FileIntegrityHasFileRequest{fileid}
	err = connection.Call("FileIntegrity.HasFile", request, &reply)

	if err != nil {
		return false
	}

	if !reply.File_Exists {
		return false
	}

	if !reply.Success {
		return false
	}
	
	return true
}

/*
 * GetAllFilesAtNode
 *
 * Returns all files stored at a given node
 */
func GetAllFilesAtNode (server string, prefix string) []string {
	connection, err := ConnectIntegrityRPC(server)
	if err != nil {
		return make([]string, 0)
	}
	defer connection.Close()

	var reply FileIntegrityResponse
	reply.Success = false
	reply.AllFiles = make([]string, 0)
	request := FileIntegrityGetAllFilesRequest{
		Prefix: prefix,
	}
	err = connection.Call("FileIntegrity.GetAllFilesAtNode", request, &reply)

	if err != nil {
		return make([]string, 0)
	}

	if !reply.Success {
		return make([]string, 0)
	}

	return reply.AllFiles
}

/*
 * ConfirmDownload
 *
 * Wrapper for calling ConfirmDownload RPC
 *
 * server- IP to request
 * filename_hash- sdfs name hashed
 * replicas- list of other nodes (unique IDs) that have this file
 */
func ConfirmDownload (server string, filename_hash string, replicas []string) error {
	connection, err := ConnectIntegrityRPC(server)
	if err != nil {
		return err
	}
	defer connection.Close()

	var reply FileIntegrityResponse
	reply.Success = false
	request := FileIntegrityConfirmRequest{filename_hash, replicas}
	err = connection.Call("FileIntegrity.ConfirmDownload", request, &reply)

	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New("Failed to confirm download")
	}

	return nil
}

/*
 * ReadRequest
 *
 * Wrapper for calling ReadRequest RPC
 *
 * server- IP to request file from
 * fileid- sdfs name hashed
 *
 * This RPC will block until a successful file transfer has completed to the IP
 * in the FileIntegrityReadRequest.IP field. May fail if file doesn't exist or is deleted.
 *
 * When this returns, the user will have the file downloaded at files/fileid (if there
 * was no error returned).
 */
func ReadRequest(server string, fileid string) error {
	connection, err := ConnectIntegrityRPC(server)
	if err != nil {
		return err
	}
	defer connection.Close()

	var reply FileIntegrityResponse
	reply.Success = false
	request := FileIntegrityReadRequest{my_ip_addr, fileid}
	err = connection.Call("FileIntegrity.ReadRequest", request, &reply)

	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New("Failed to perform read request")
	}

	return nil
}

/*
 * RemoveRequest
 *
 * Wrapper for calling RemoveRequest RPC
 */
func RemoveRequest (server string, fileid string) error {
	connection, err := ConnectIntegrityRPC(server)
	if err != nil {
		return err
	}
	defer connection.Close()

	var reply FileIntegrityResponse
	reply.Success = false
	request := FileIntegrityRemoveRequest{fileid}
	err = connection.Call("FileIntegrity.RemoveRequest", request, &reply)

	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New("Failed to delete file")
	}
	
	return nil
}

/*
 * AppendRequest
 *
 * Request that a remote server append some string to a given file.
 *
 * Assumption: you hold the write lock to this file.
 */
func AppendRequest(server string, filename_hash string, filename_plain string, append_data []byte) error {
	connection, err := ConnectIntegrityRPC(server)
	if err != nil {
		return err
	}
	defer connection.Close()

	var reply FileIntegrityResponse
	reply.Success = false
	request := FileIntegrityAppendRequest{
		Filename_Hash: filename_hash,
		Filename_Plain: filename_plain,
		AppendData: append_data,
	}
	err = connection.Call("FileIntegrity.AppendRequest", request, &reply)

	if err != nil {
		return err
	}

	if !reply.Success {
		return errors.New("Failed to append to file")
	}

	return nil
}

/******************
 *  RPC Handlers  *
 ******************/

/*
 * FileIntegrity::HasFile
 *
 * Returns whether or not this node has this file in its my_files table
 */
func (fi *FileIntegrity) HasFile (req FileIntegrityHashRequest, resp *FileIntegrityResponse) error {
	my_files.RLock()
	_, resp.File_Exists = my_files.m[req.Filename_Hash]
	my_files.RUnlock()

	resp.Success = true

	return nil
}

/*
 * FileIntegrity::GetHash
 *
 * Returns the md5 hash of a file and whether it exists
 */
func (fi *FileIntegrity) GetHash (req FileIntegrityHashRequest, resp *FileIntegrityResponse) error {
	file, err := os.Open(path.Join("files", req.Filename_Hash))

	// Couldn't find file
	if err != nil {
		resp.File_Exists = false
		resp.Hash = ""
		return err
	}
	file.Close()

	resp.File_Exists = true

	// Get its hash
	file_hash, err := GetFileHash(path.Join("files", req.Filename_Hash))
	if err != nil {
		return err
	}
	resp.Hash = file_hash
	resp.Success = true

	return nil
}

/*
 * FileIntegrity::GetAllFilesAtNode
 *
 * Returns a list of all files stored at this node.
 * Returns names in SDFS name (NOT hashes!)
 */
func (fi *FileIntegrity) GetAllFilesAtNode (req FileIntegrityGetAllFilesRequest, resp *FileIntegrityResponse) error {
	all_files := make([]string, 0)
	my_files.RLock()
	hash_to_names.RLock()

	for key, _ := range my_files.m {
		if _, present := hash_to_names.m[key]; present {
			if strings.HasPrefix(hash_to_names.m[key], req.Prefix) {
				all_files = append(all_files, hash_to_names.m[key])
			}
		} else {
			// Somehow a file didn't show up in hash_to_names
			fmt.Println("hash_to_names is incomplete!!! This should never ever happen")
		}
	}

	hash_to_names.RUnlock()
	my_files.RUnlock()

	resp.Success = true
	resp.AllFiles = all_files

	return nil
}

/*
 * FileIntegrity::ConfirmDownload
 *
 * Confirm a download is valid (hashes match and the sender was
 * able to release the token successfully).
 */
func (fi *FileIntegrity) ConfirmDownload (req FileIntegrityConfirmRequest, resp *FileIntegrityResponse) error {
	my_files.Lock()

	// @TODO: Ensure the file exists

	my_files.m[req.Filename_Hash] = req.Replicas

	my_files.Unlock()

	resp.Success = true

	return nil
}

/*
 * FileIntegrity::ReadRequest
 *
 * Request a file be transferred at once!
 */
func (fi *FileIntegrity) ReadRequest (req FileIntegrityReadRequest, resp *FileIntegrityResponse) error {
	hash_to_names.RLock()
	if _, present := hash_to_names.m[req.Filename_Hash]; !present {
		fmt.Println("Error: hash to files doesn't have a record for hash", req.Filename_Hash)
		return errors.New("We don't know what this file is named!")
	}
	sdfsname := hash_to_names.m[req.Filename_Hash]
	hash_to_names.RUnlock()

	err := SendFile(req.Sender_IP, path.Join("files", req.Filename_Hash), req.Filename_Hash, sdfsname)

	if err != nil {
		return err
	}

	resp.Success = true

	return nil
}

/*
 * FileIntegrity::RemoveRequest
 *
 * Request a file be deleted, if it exists
 */
func (fi *FileIntegrity) RemoveRequest (req FileIntegrityRemoveRequest, resp *FileIntegrityResponse) error {
	my_files.Lock()
	defer my_files.Unlock()

	if _, present := my_files.m[req.Filename_Hash]; present {
		delete(my_files.m, req.Filename_Hash)
	}

	if _, err := os.Stat(path.Join("files", req.Filename_Hash)); err == nil {
		os.Remove(path.Join("files", req.Filename_Hash))
	}

	resp.Success = true

	return nil
}

/*
 * FileIntegrity::AppendRequest
 *
 * Append a string to a file stored.
 */
func (fi *FileIntegrity) AppendRequest (req FileIntegrityAppendRequest, resp *FileIntegrityResponse) error {
	my_files.Lock()
	defer my_files.Unlock()

	resp.Success = false

	// Not found in files
	if _, err := os.Stat(path.Join("files", req.Filename_Hash)); os.IsNotExist(err) {
		file, err := os.Create(path.Join("files", req.Filename_Hash))
		if err != nil {
			fmt.Println("error creating file from append")
			fmt.Println(err)
			return err
		}
		file.Close()

		hash_to_names.Lock()
		hash_to_names.m[req.Filename_Hash] = req.Filename_Plain
		hash_to_names.Unlock()
	}

	// File should be at files/filenamehash
	file, err := os.OpenFile(path.Join("files", req.Filename_Hash), os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	_, err = file.Write(req.AppendData)
	if err != nil {
		return err
	}

	err = file.Sync()
	if err != nil {
		return err
	}

	err = file.Close()
	if err != nil {
		return err
	}

	resp.Success = true
	return nil
}
