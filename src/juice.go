package main

import (
	"fmt"
	"sync"
	"time"
	"errors"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"math"
	"os/exec"
	"sort"
)

/*
 * Files used by juice
 *
 * Added to SDFS:
 * __EXE__{prefix} 			:	the exe to run
 * __OUT__{prefix}_{key} 	: 	output for key {key}
 *
 * Created locally in workdir at work node:
 * workdir/__EXE__{prefix}
 * workdir/{prefix}_{key}
 */

/*
 * RANGE_PARTITIONING
 *
 * When set to true, juice uses range based partitioning.
 * When false, it uses random ("""hashed""") partitioning.
 */
var RANGE_PARTITIONING = true

/*
 * StartJuice
 *
 * Launch a juice phase. This wraps the "Juice" function
 *
 * Inputs:
 *  - host_juice_exe: Host name of the binary to run
 *  - num_juices: Number of juice jobs to launch
 *  - sdfs_prefix: SDFS name prefix
 *  - host_dst_dir: Host directory to store results in
 *  - delete_files: Do we delete intermediate files after?
 *
 * Outputs:
 *  - Error code on failure, nil on success
 *
 * Assumptions:
 * For now, assume that only the master node can call StartJuice.
 */
func StartJuice(host_juice_exe string, num_juices int, sdfs_prefix string, host_dst_dir string, delete_files bool) error {
	// Step 0: Wait for permission from master
	var allowed = false
	allow_num_retries := 100
	for !allowed {
		allowed = RequestMapleJuicePerm(my_ip_addr)
		allow_num_retries -= 1
		if allow_num_retries == 0 {
			fmt.Println("Timed out waiting to get permission to use MapleJuice")
			return errors.New("timeout")
		}
		time.Sleep(time.Millisecond * 1000)
	}
	defer ReleaseMapleJuicePerm(my_ip_addr)
	
	// Step 1: Count number of key files to parse
	intermediate_files := GetAllFiles(sdfs_prefix)

	// Step 2: Partition into num_juices workers
	// How many jobs are really needed?
	// If there are 3 lines of input and we ask for 4 juice jobs, we only need 3
	actual_jobs_needed := num_juices
	if num_juices > len(intermediate_files) {
		actual_jobs_needed = len(intermediate_files)
	}

	// Hash partitioning / range partitioning
	keys := make([]string, 0)
	for _, filename := range intermediate_files {
		key := strings.Split(filename, sdfs_prefix + "_")[1]
		keys = append(keys, key)
	}

	// Partition keys
	// map job index -> list of keys
	job_assignments := make(map[int][]string)

	// Assign keys_per_job keys to each job
	keys_per_job := int(math.Ceil(float64(len(keys)) / float64(actual_jobs_needed)))
	// fmt.Println("Keys:", keys)
	// fmt.Println("Actual jobs needed:", actual_jobs_needed)
	// fmt.Println("Keys per job:", keys_per_job)

	// If we want range partitioning, we sort the keys slice before assinging in order
	if RANGE_PARTITIONING {
		sort.Strings(keys)
	}

	job_counter := 0
	keys_assigned := 0
	for _, key := range keys {
		job_assignments[job_counter] = append(job_assignments[job_counter], key)
		keys_assigned += 1
		if keys_assigned == keys_per_job {
			keys_assigned = 0
			job_counter += 1
		}
	}

	for idx, keys := range job_assignments {
		fmt.Println("Job", idx, "has keys", keys)
	}

	completely_successful := false
	juice_retries := MAX_RETRIES
	var err error

	exe_sdfs_name := "__EXE__" + sdfs_prefix

	for !completely_successful {
		// Step 3: Push EXE to SDFS
		if juice_retries <= 0 {
			fmt.Println("Error: too many retries")
			return errors.New("Too many retries")
		}

		err = Delete(exe_sdfs_name)
		if err != nil {
			juice_retries -= 1
			fmt.Println("---> Error in Juice Delete EXE")
			time.Sleep(time.Millisecond * 1000)
			continue
		}

		err = Put(host_juice_exe, exe_sdfs_name)
		if err != nil {
			juice_retries -= 1
			fmt.Println("---> Error in Juice Put EXE")
			time.Sleep(time.Millisecond * 1000)
			continue
		}

		// Step 4: Launch juice
		err = Juice(exe_sdfs_name, job_assignments, sdfs_prefix)

		// Step 5: If Juice was successful and we should delete things delete them here
		if err == nil {
			completely_successful = true
			break
		}

		if err != nil {
			juice_retries -= 1
			fmt.Println("---> Error in Juice Run")
			time.Sleep(time.Millisecond * 3000)
			continue
		}
	}

	if delete_files {
		intermediate_files = GetAllFiles(sdfs_prefix)

		for _, file := range intermediate_files {
			err = Delete(file)
			if err != nil {
				fmt.Println("Error deleting intermediate file? Huh")
				fmt.Println(err)
				return err
			}
		}
	}

	// Copy files to output host directory
	for _, key := range keys {
		// Create output directory if missing
		_, err := os.Stat(host_dst_dir)
		if os.IsNotExist(err) {
			err_dir := os.MkdirAll(host_dst_dir, 0755)
			if err_dir != nil {
				fmt.Println("Couldn't create output directory")
				return err_dir
			}
		}

		out_path := filepath.Join(host_dst_dir, key)
		sdfs_name := "__OUT__" + sdfs_prefix + "_" + key
		fmt.Println("Copying", sdfs_name, "to", host_dst_dir)
		err = Get(sdfs_name, out_path)
		if err != nil {
			fmt.Println("Error pulling result from Juice")
			fmt.Println(err)
			// return err
		}

		Delete(sdfs_name)
	}

	Delete(exe_sdfs_name)

	return nil
}

/*
 * Juice
 *
 * Launch a juice phase.
 *
 * Inputs:
 *  - juice_exe: SDFS name of the binary to run
 *  - job_assignments: map of id -> []string containing all keys to use
 *  - sdfs_prefix: SDFS name prefix
 *
 * Outputs:
 *  - Error code on failure, nil on success
 *
 * Assumptions:
 *  1. For now, assume that only the master node can call Juice.
 */
func Juice(juice_exe string, job_assignments map[int][]string, sdfs_prefix string) error {
	var wg sync.WaitGroup
	responses := make(chan error)

	num_juices := len(job_assignments)

	// Launch the worker threads
	for i := 0; i < num_juices; i++ {
		wg.Add(1)

		go func (juice_id_in int, keys_in []string) {
			defer wg.Done()
			responses <- JuiceWorker(juice_exe, juice_id_in, keys_in, sdfs_prefix)
		}(i, job_assignments[i])
	}

	// Wait for all goroutines to synchronize
	go func () {
		wg.Wait()
		close(responses)
	}()

	// See which ones failed
	for err := range responses {
		if err != nil {
			fmt.Println("We had a worker fail!")
			fmt.Println(err)
			return err
		}
	}

	// Success
	return nil
}

/*
 * JuiceWorker
 *
 * Spins until it finds a free node, and once it does, schedules a job.
 * There is one of these per num_juices.
 *
 * Returns nil on success, and some error code on error.
 * This should be called as a goroutine and its error code should be checked
 */
func JuiceWorker (juice_exe string, juice_id int, keys []string, sdfs_prefix string) error {
	fmt.Println("Worker", juice_id, "is alive!")

	var err error
	free_server_ip := ""

	retries_remaining := 0
	var completely_successful = false

	for !completely_successful {
		// Spin until we aquire a free node
		// See juice.go for the node pairing logic
		// spin_start := time.Now()
		for {
			free_server_ip, err = PickFreeNode()

			// Got one
			if err == nil {
				break
			}

			// Error isn't that we are busy, so break
			if err != all_nodes_busy {
				fmt.Println(err)
				return err
			}

			// Potentially time out
			// if time.Since(spin_start).Seconds() > MAPLE_TIMEOUT {
			// 	return errors.New("Timeout waiting for free servers")
			// }
		}

		fmt.Println("[", juice_id, "] Scheduling juice job at", free_server_ip)
		success, _ := RunJuice(free_server_ip, juice_exe, keys, sdfs_prefix)
		fmt.Println("[", juice_id, "] Juice job completed at", free_server_ip)

		// Free node after it returns even if it failed
		MarkNodeAsFree(free_server_ip)

		if success == true {
			// Done! We can go home now
			completely_successful = true
			break
		}

		if retries_remaining == 0 {
			return errors.New("Juice Worker failed")
		}
		retries_remaining -= 1

		// Sleep for a bit before immediately retrying
		time.Sleep(time.Millisecond * 3000)
	}

	return err
}

/*********************
 *  Network Objects  *
 *********************/

/*
 * Network synchronization object. Not used as an object anywhere- just used for RPCs
 */
type JuiceRPC struct { }

/*
 * ConnectJuiceRPC
 *
 * Master uses this to connect to Juice job servers
 */
func ConnectToJuiceJob (server string) (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", server + ":5051")

	if err != nil {
		fmt.Print(server, " not responding (ConnectToJuiceJob)\n")
	}

	return client, err
}

/*
 * A juice job request, used by master to request a juice job
 */
type JuiceJobRequest struct {
	JuiceExe string `json:"juice_exe"`

	// All keys to use are sent as a slice:
	JuiceKeys []string `json:"juice_keys"`

	SDFSPrefix string `json:"sdfs_prefix"`
}

/*
 * A juice job response
 */
type JuiceJobReply struct {
	Success bool `json:"success"`
}

/*
 * JuiceRPCServer
 *
 * Listen on port 5051 for RPCs
 */
func JuiceRPCServer() error {
	juiceState := new(JuiceRPC)

	// Setup TCP listener:
	addr, err := net.ResolveTCPAddr("tcp", ":5051")

	if err != nil {
		fmt.Println("Error", err)
		return err
	}

	inbound, err := net.ListenTCP("tcp", addr)

	if err != nil {
		fmt.Println("Error", err)
		return err
	}

	rpc.Register(juiceState)

	// Loop on accepting inputs
	rpc.Accept(inbound)

	return nil
}

/******************
 *  RPC Wrappers  *
 ******************/

/*
 * RunJuice
 *
 * Launch a juice job on a given job server.
 * Returns (success, error) where success is a bool indicating whether the operation
 * succeeded, and error is an error code (should be nil on success)
 */
func RunJuice(server string, juice_exe string, keys []string, sdfs_prefix string) (bool, error) {
	connection, err := ConnectToJuiceJob(server)
	if err != nil {
		return false, err
	}
	defer connection.Close()

	var reply JuiceJobReply
	reply.Success = false
	request := JuiceJobRequest{
		JuiceExe: juice_exe,
		JuiceKeys: keys,
		SDFSPrefix: sdfs_prefix,
	}

	err = connection.Call("JuiceRPC.RunJuice", request, &reply)

	return reply.Success, err
}

/*
 * RunJuice
 *
 * Runs a single juice job with a given index and exe
 */
func (m *JuiceRPC) RunJuice (req JuiceJobRequest, reply *JuiceJobReply) error {
	fmt.Println("Running juice task for keys", req.JuiceKeys)

	// Step 1: Create work directory, pull our input file and exe from SDFS
	// Create work directory if missing
	_, err := os.Stat("workdir")
	if os.IsNotExist(err) {
		err_dir := os.MkdirAll("workdir", 0755)
		if err_dir != nil {
			fmt.Println("Couldn't create work directory")
			return err_dir
		}
	}

	sdfs_exe := "__EXE__" + req.SDFSPrefix
	local_exe := filepath.Join("workdir", sdfs_exe)

	err = Get(sdfs_exe, local_exe)
	if err != nil {
		fmt.Println("Failed to fetch exe for job")
		fmt.Println(err)
		return err
	}

	os.Chmod(local_exe, 0655) // What do you mean "security"?

	for _, key := range req.JuiceKeys {
		// Step 2: Run exe with input file and store output
		sdfs_input_name := req.SDFSPrefix + "_" + key
		sdfs_output_name := "__OUT__" + req.SDFSPrefix + "_" + key
		local_input_file := filepath.Join("workdir", sdfs_input_name)

		err = Get(sdfs_input_name, local_input_file)
		if err != nil {
			fmt.Println("Failed to get input file")
			fmt.Println(err)
			return err
		}

		output_bytes, err := exec.Command(local_exe, local_input_file).Output()
		if err != nil {
			fmt.Println("Error executing juice exe")
			fmt.Println(err)
			return err
		}

		output_string := string(output_bytes)

		// Give us time to kill this task:
		// time.Sleep(time.Millisecond * 3000)

		Delete(sdfs_output_name)

		// Create the file
		// tmp_file_path := filepath.Join("workdir", sdfs_output_name)
		// tmp_file, err := os.Create(tmp_file_path)
		// if err != nil {
		// 	fmt.Println("Error creating workdir temp file")
		// 	return err
		// }
		// tmp_file.Close()

		// err = Put(tmp_file_path, sdfs_output_name)
		// if err != nil {
		// 	fmt.Println("Error putting output file")
		// 	return err
		// }

		// Append to newly created file (just simpler this way)
		fmt.Println("Pushing", sdfs_output_name)
		err = Append(sdfs_output_name, []byte(output_string))
		if err != nil {
			fmt.Println("Error appending")
			fmt.Println(err)
			return err
		}
	}

	fmt.Println("Juice task complete!")

	reply.Success = true
	return nil
}
