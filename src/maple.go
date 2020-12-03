package main

import(
	"fmt"
	"os"
	"io"
	"path/filepath"
	"math"
	"bufio"
	"strings"
	"strconv"
	"net/rpc"
	"errors"
	"time"
	"sync"
	"net"
	"os/exec"
)

/*
 * Files used by maple
 *
 * Created locally in input directory at launching node:
 * .__INPUT__ 			: 	all inputs collected into one file
 * .__INPUT__N 			: 	an input set for job N
 *
 * Added to SDFS:
 * __EXE__{prefix} 		:	the exe to run
 * __JOB__{prefix}_N 	:	the input set for job N
 * {prefix}_{key}		: 	intermediate files output from map of key {key}
 *
 * Created locally in workdir at work node:
 * workdir/__EXE__{prefix}
 * workdir/__JOB__{prefix}_N
 */

/*
 * busy_members
 *
 * Maps Member ID -> is busy
 * Where member ID is a key in the members.m map
 *
 * Must aquire members lock first.
 */
var busy_members = make(map[string]bool)

var all_nodes_busy = errors.New("No free nodes")

var MAPLE_TIMEOUT = 120.0

// How many times will a worker thread retry until it gives up
var MAX_RETRIES = 5

/*
 * StartMaple
 *
 * Launch a maple phase. This wraps the "Maple" function and combines all files in the host_src_dir
 * folder into one single file, puts it into SDFS, and then calls Maple with that file as the name.
 *
 * Inputs:
 *  - host_maple_exe: Host name of the binary to run
 *  - num_maples: Number of maple jobs to launch
 *  - sdfs_prefix: SDFS name prefix
 *  - host_src_dir: Host directory containing a number of files that act as input to maple
 *
 * Outputs:
 *  - Error code on failure, nil on success
 *
 * Assumptions:
 * For now, assume that only the master node can call StartMaple.
 */
func StartMaple(host_maple_exe string, num_maples int, sdfs_prefix string, host_src_dir string) error {
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

	// Step 1: combine everything in host_src_dir into one file
	host_file_name := ".__INPUT__"

	// Create combined file in {host_src_dir}/.__INPUT__
	host_file_path := filepath.Join(host_src_dir, host_file_name)
	host_file, err := os.Create(host_file_path)
	if err != nil {
		fmt.Println("Error creating combined file")
		return err
	}

	// Write all inputs into combined file
	err = filepath.Walk(host_src_dir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && path != host_file_path && !strings.HasPrefix(filepath.Base(path), ".__INPUT__") && filepath.Base(path) != ".DS_Store" {
			//fmt.Println("Opening", path)

			cur_file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer cur_file.Close()

			io.Copy(host_file, cur_file)
		}
		return nil
	})

	if err != nil {
		return err
	}

	host_file.Close()
	fmt.Println("Step 1 done")

	// Step 2: Partition that file into num_maples files and store them all in SDFS
	// First, count the number of lines in the mega input file
	num_lines, err := CountLinesInFile(host_file_path)
	if err != nil {
		return err
	}

	lines_per_job := int(math.Ceil(float64(num_lines) / float64(num_maples)))
	fmt.Println("We have", num_lines, "lines of input\nSo that means", lines_per_job, "lines per job")

	host_file, err = os.Open(host_file_path)
	if err != nil {
		return err
	}
	defer host_file.Close()
	host_file_reader := bufio.NewReader(host_file)

	// How many jobs are really needed?
	// If there are 3 lines of input and we ask for 4 maple jobs, we only need 3
	actual_jobs_needed := 0

	lines_left_to_assign := num_lines
	for i := 0; i < num_maples; i++ {
		//fmt.Println("Assigning to job", i)
		cur_job_path := filepath.Join(host_src_dir, ".__INPUT__" + strconv.Itoa(i))
		cur_job_file, err := os.Create(cur_job_path)
		if err != nil {
			return err
		}

		lines_written := 0
		for cur_line := 0; cur_line < lines_per_job; cur_line++ {
			if lines_left_to_assign > 0 {
				line, err := host_file_reader.ReadBytes('\n')
				if err != nil {
					return err
				}

				cur_job_file.Write(line)

				//fmt.Println("(", i, ",", cur_line, "):", string(line))
				lines_left_to_assign -= 1
				lines_written += 1
			}
		}

		cur_job_file.Close()

		if lines_written != 0 {
			actual_jobs_needed += 1
		}
	}
	fmt.Println("Step 2 done")

	num_retries := MAX_RETRIES
	completely_successful := false

	exe_sdfs_name := "__EXE__" + sdfs_prefix

	for !completely_successful {
		// Step 3: Push EXE to SDFS
		num_exe_put_retries := 10
		exe_put_success := false
		Delete(exe_sdfs_name)
		for !exe_put_success {
			err = Put(host_maple_exe, exe_sdfs_name)
			if err == nil {
				exe_put_success = true
				break
			}

			num_exe_put_retries -= 1
			if num_exe_put_retries == 0 {
				fmt.Println("!! Couldn't Put !!")
				return err
			}
		}
		fmt.Println("Step 3 done")

		// Step 3.5: Push intermediate files
		for cur_job_put_id := 0; cur_job_put_id < actual_jobs_needed; cur_job_put_id++ {
			num_put_retries := 10
			put_success := false
			cur_job_path := filepath.Join(host_src_dir, ".__INPUT__" + strconv.Itoa(cur_job_put_id))
			for !put_success {
				num_del_retries := 10
				del_success := false
				for !del_success {
					err = Delete("__JOB__" + sdfs_prefix + "_" + strconv.Itoa(cur_job_put_id))
					if err == nil {
						del_success = true
					}
					num_del_retries -= 1
					if num_del_retries == 0 {
						break
					}

					time.Sleep(time.Millisecond * 250)
				}

				err = Put(cur_job_path, "__JOB__" + sdfs_prefix + "_" + strconv.Itoa(cur_job_put_id))
				if err == nil {
					put_success = true
					break
				}

				fmt.Println("Error detected in Put (229), retrying...")
				fmt.Println(err)

				num_put_retries -= 1
				if num_put_retries == 0 {
					fmt.Println("!! Couldn't Put !!")
					return err
				}

				time.Sleep(time.Millisecond * 500)
			}
		}

		// Step 4: Clear SDFS of any intermediate files
		existing_files := GetAllFiles(sdfs_prefix)
		for _, existing_file := range existing_files {
			err = Delete(existing_file)
			if err != nil {
				fmt.Println("Error found cleaning up previous round with Delete")
				fmt.Println(err)
				return err
			}
		}
		fmt.Println("Step 4 done")

		// Step 5: Launch maple
		err = Maple(exe_sdfs_name, actual_jobs_needed, sdfs_prefix)
		if err == nil {
			completely_successful = true
			break
		}

		if num_retries == 0 {
			fmt.Println("Out of retries")
			fmt.Println(err)
			return errors.New("Too many retries")
		}
		num_retries -= 1

		time.Sleep(time.Millisecond * 5000)
		fmt.Println("RETRYING")
	}

	fmt.Println("Step 5 done")

	// We were successful- delete all job files
	for i := 0; i < actual_jobs_needed; i++ {
		Delete("__JOB__" + sdfs_prefix + "_" + strconv.Itoa(i))
	}
	Delete(exe_sdfs_name)

	return nil
}

/*
 * Maple
 *
 * Launch a maple phase.
 *
 * Inputs:
 *  - maple_exe: SDFS name of the binary to run
 *  - num_maples: Number of maple jobs to launch (assumption: min number of jobs required)
 *  - sdfs_prefix: SDFS name prefix
 *
 * Outputs:
 *  - Error code on failure, nil on success
 *
 * Assumptions:
 *  1. For now, assume that only the master node can call Maple.
 *  2. Each of the num_maples tasks has a file called {sdfs_prefix}__INPUT__{N} already stored in SDFS
 */
func Maple(maple_exe string, num_maples int, sdfs_prefix string) error {
	var wg sync.WaitGroup
	responses := make(chan error)

	// Launch the worker threads
	fmt.Println("Launching workers")
	for i := 0; i < num_maples; i++ {
		wg.Add(1)

		go func (maple_id_in int) {
			defer wg.Done()
			responses <- MapleWorker(maple_exe, maple_id_in, sdfs_prefix)
		}(i)
	}

	// Wait for all goroutines to synchronize
	go func () {
		wg.Wait()
		close(responses)
	}()

	// See which ones failed
	// any_failures := false
	for err := range responses {
		if err != nil {
			fmt.Println("Error in Maple worker")
			fmt.Println(err)
			return err
		}
	}

	// Success
	return nil
}

/*
 * MapleWorker
 *
 * Spins until it finds a free node, and once it does, schedules a job.
 * There is one of these per num_maples.
 *
 * Returns nil on success, and some error code on error.
 * This should be called as a goroutine and its error code should be checked
 */
func MapleWorker (maple_exe string, maple_id int, sdfs_prefix string) error {
	fmt.Println("Worker", maple_id, "is alive!")

	var err error
	free_server_ip := ""

	retries_remaining := 0
	var completely_successful = false

	for !completely_successful {
		// Spin until we aquire a free node
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

		fmt.Println("[", maple_id, "] Scheduling maple job at", free_server_ip)
		success, err := RunMaple(free_server_ip, maple_exe, maple_id, sdfs_prefix)
		fmt.Println("[", maple_id, "] Job finished at", free_server_ip)

		// Free node after it returns even if it failed
		MarkNodeAsFree(free_server_ip)

		if err != nil {
			fmt.Println("Error in worker thread", maple_id)
			fmt.Println(err)
			return err
		}

		if success == true {
			// Done! We can go home now
			completely_successful = true
			break
		}

		if retries_remaining == 0 {
			return errors.New("Could not complete maple run, too many failed attempts")
		}
		retries_remaining -= 1

		// Sleep for a bit before immediately retrying
		time.Sleep(time.Millisecond * 3000)
		fmt.Println("Worker", maple_id, "retrying")
	}

	// We were successful so we can remove that interemediate file:
	//Delete("__JOB__" + sdfs_prefix + "_" + strconv.Itoa(maple_id))

	return err
}

/*
 * PickFreeNode
 *
 * Returns the IP address of a non-busy node.
 *
 * Aquires the members read lock!
 */
func PickFreeNode() (string, error) {
	members.Lock()
	defer members.Unlock()
	for id, val := range members.m {
		busy_status, present := busy_members[id]

		if !present || busy_status == false {
			if val.IP != my_ip_addr {
				// Never assign jobs to the master
				busy_members[id] = true
				return val.IP, nil
			}
		}
	}

	return "", all_nodes_busy
}

/*
 * MarkNodeAsFree
 *
 * If this node exists in busy_members we remove it.
 *
 * Aquires the members write lock!
 */
func MarkNodeAsFree (ip_to_rm string) {
	members.Lock()
	defer members.Unlock()

	for key, member := range members.m {
		if member.IP == ip_to_rm {
			if _, present := busy_members[key]; present {
				delete(busy_members, key)
			}
		}
	}
}

/*********************
 *  Network Objects  *
 *********************/

/*
 * Network synchronization object. Not used as an object anywhere- just used for RPCs
 */
type MapleRPC struct { }

/*
 * ConnectMapleRPC
 *
 * Master uses this to connect to Maple job servers
 */
func ConnectToMapleJob (server string) (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", server + ":5050")

	if err != nil {
		fmt.Print(server, " not responding (ConnectToMapleJob)\n")
	}

	return client, err
}

/*
 * A maple job request, used by master to request a maple job
 */
type MapleJobRequest struct {
	MapleExe string `json:"maple_exe"`
	MapleID int `json:"maple_id"`
	SDFSPrefix string `json:"sdfs_prefix"`
}

/*
 * A maple job response
 */
type MapleJobReply struct {
	Success bool `json:"success"`
}

/*
 * MapleRPCServer
 *
 * Listen on port 5050 for RPCs
 */
func MapleRPCServer() error {
	mapleState := new(MapleRPC)

	// Setup TCP listener:
	addr, err := net.ResolveTCPAddr("tcp", ":5050")

	if err != nil {
		fmt.Println("Error", err)
		return err
	}

	inbound, err := net.ListenTCP("tcp", addr)

	if err != nil {
		fmt.Println("Error", err)
		return err
	}

	rpc.Register(mapleState)

	// Loop on accepting inputs
	rpc.Accept(inbound)

	return nil
}

/******************
 *  RPC Wrappers  *
 ******************/

/*******************
 *   Master RPCs   *
 *   Talk to jobs  *
 *   on Port 5050  *
 *******************/

/*
 * RunMaple
 *
 * Launch a maple job on a given job server.
 * Returns (success, error) where success is a bool indicating whether the operation
 * succeeded, and error is an error code (should be nil on success)
 */
func RunMaple(server string, maple_exe string, maple_id int, sdfs_prefix string) (bool, error) {
	connection, err := ConnectToMapleJob(server)
	if err != nil {
		return false, err
	}
	defer connection.Close()

	var reply MapleJobReply
	reply.Success = false
	request := MapleJobRequest{
		MapleExe: maple_exe,
		MapleID: maple_id,
		SDFSPrefix: sdfs_prefix,
	}

	err = connection.Call("MapleRPC.RunMaple", request, &reply)

	return reply.Success, err
}

/******************
 *  RPC Handlers  *
 ******************/

/*
 * MapleElement
 * Data structure used to represent a single line output from maple exe.
 */
type MapleElement struct {
	Key string
	Val string
}

/*
 * RunMaple
 *
 * Runs a single maple job with a given index and exe
 */
func (m *MapleRPC) RunMaple (req MapleJobRequest, reply *MapleJobReply) error {
	fmt.Println("Running maple task", req.MapleID)

	// Step 1: Create work directory, pull our input file and exe from SDFS
	// Create work directory if missing
	os.RemoveAll("workdir")
	_, err := os.Stat("workdir")
	if os.IsNotExist(err) {
		err_dir := os.MkdirAll("workdir", 0755)
		if err_dir != nil {
			fmt.Println("Couldn't create work directory")
			return err_dir
		}
	}

	sdfs_input_name := "__JOB__" + req.SDFSPrefix + "_" + strconv.Itoa(req.MapleID)
	local_input_file := filepath.Join("workdir", sdfs_input_name)

	sdfs_exe := "__EXE__" + req.SDFSPrefix
	local_exe := filepath.Join("workdir", sdfs_exe)

	err = Get(sdfs_exe, local_exe)
	if err != nil {
		fmt.Println("Failed to fetch exe for job")
		fmt.Println(err)
		return err
	}

	err = Get(sdfs_input_name, local_input_file)
	if err != nil {
		fmt.Println("Failed to fetch job file for job", req.MapleID)
		fmt.Println(err)
		return err
	}

	// Step 2: Run exe with input file and store output
	os.Chmod(local_exe, 0655) // What do you mean "security"?
	output_bytes, err := exec.Command(local_exe, local_input_file).Output()
	if err != nil {
		fmt.Println("Error executing maple exe")
		fmt.Println(err)
		return err
	}

	output := string(output_bytes)

	// Step 3: Append output to key files, creating file if it doesn't exist
	scanner := bufio.NewScanner(strings.NewReader(output))

	// Maps Keys -> Values
	var output_list []MapleElement

	for scanner.Scan() {
		line := scanner.Text()

		var element MapleElement

		element.Key = strings.Split(strings.Split(line, "(")[1], ",")[0]
		element.Val = strings.Split(strings.Split(line, ",")[1], ")")[0]

		output_list = append(output_list, element)
	}

	fmt.Println("Processed", len(output_list), "elements")

	// Get all unique keys
	unique_key_helper_map := make(map[string]bool)
	unique_keys := make([]MapleElement, 0)
	for _, val := range output_list {
		if _, present := unique_key_helper_map[val.Key]; !present {
			unique_key_helper_map[val.Key] = true
			unique_keys = append(unique_keys, val)
		}
	}

	fmt.Println("Completed processing. Uploading data...")
	for _, unique_element := range unique_keys {
		this_sdfs_file_name := req.SDFSPrefix + "_" + unique_element.Key

		/*existing_file := GetAllFiles(this_sdfs_file_name)

		if len(existing_file) == 0 {
			// Create the file
			tmp_file_path := filepath.Join("workdir", this_sdfs_file_name)
			tmp_file, err := os.Create(tmp_file_path)
			if err != nil {
				fmt.Println("Error creating workdir temp file")
				return err
			}
			tmp_file.Close()

			// Keep retrying to put it until we succeed or detect the file elsewhere
			found_file := false
			for !found_file {
				fmt.Println("Putting", this_sdfs_file_name)
				err = Put(tmp_file_path, this_sdfs_file_name)
				if err == nil {
					found_file = true
				}

				if len(GetAllFiles(this_sdfs_file_name)) != 0 {
					found_file = true
				}
			}
		}*/

		// Create an aggregate output string
		output_string := ""
		for _, element := range output_list {
			if element.Key == unique_element.Key {
				output_string += "(" + element.Key + "," + unique_element.Val + ")\n"
			}
		}

		// Append (this will spin and not fail due to lock exhaustion)
		fmt.Println("Appending", this_sdfs_file_name)
		err = Append(this_sdfs_file_name, []byte(output_string))
		if err != nil {
			fmt.Println("Error appending")
			fmt.Println(err)
			return err
		}
	}

	fmt.Println("All data uploaded")

	// Step 4: Delete our input file from SDFS if everything succeeded
	reply.Success = true
	return nil
}
