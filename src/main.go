package main

/*
 * ECE 428 Distributed Systems MP 3
 *
 * MapleJuice
 *
 * Joseph Ravichandran
 * Anchit Rao
 * Naveen Nathan
 */

/*
 * Network State
 *
 * Port 5050: Maple RPC service (see maple.go)
 * Port 5051: Juice RPC service (see juice.go)
 * Port 6060: UDP Membership service (see membership.go)
 * Port 7070: Token RPC Service (master listens on here) (node - master RPCs)
 * Port 8080: File IO Service (everyone listens on here)
 * Port 9090: File Integrity RPC Service (nodes listen on here) (node - node RPCs)
 */

import (
	"fmt"
	"flag"
	"time"
	"math/rand"
	"bufio"
	"os"
	"strings"
	"crypto/md5"
	"strconv"
	"os/exec"
)

/*
 * Global state
 */
// Unique ID of the master
var master_id string

/*
 * main entrypoint
 */
func main () {
	// Seed randomizer:
	rand.Seed(time.Now().UnixNano())

	// Switch between introducer and regular node
	isIntroducer := flag.String("i", "localhost", "Specify the introducer node address")
	flag.Parse()

	// Launch integrity check server
	go IntegrityRPCServer()

	// Launch master RPC handler, even if we aren't the master
	go MasterRPCServer()

	// Launch file handler server
	go FileServer()

	// Launch MapleJuice RPC servers
	go MapleRPCServer()
	go JuiceRPCServer()

	GetNetworkInfo()

	// Join membership network
	if (*isIntroducer == "localhost") {
		master_id = my_ip_addr
		JoinMembershipSystem("localhost")
	} else {
		// We'll get the ID from the introducer
		master_id = "x"
		JoinMembershipSystem(*isIntroducer)
	}

	// Wait a bit for everything to kick off
	time.Sleep(time.Millisecond * 100)

	// Run tests
	// Test append:
	// time.Sleep(time.Millisecond * 5000)
	// if (*isIntroducer == "localhost") {
	// 	Put("file.txt", "testfile")
	// } else {
	// 	time.Sleep(time.Millisecond * 5000)
	// 	fmt.Println(my_ip_addr, "is testing append")
	// 	AppendTest()
	// 	AppendTest()
	// 	AppendTest()
	// }

	// Step 4: Handle any user input
	io_reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("> ")
		user_input, _ := io_reader.ReadString('\n')
		user_input = strings.ReplaceAll(user_input, "\n", "")

		args := strings.Split(user_input, " ")

		if user_input == "show master" {
			fmt.Println(master_id)
		}

		if strings.Contains(user_input, "put") {
			// put {localname} {sdfsname}
			if len(args) == 3 {
				err := Put(args[1], args[2])

				if err != nil {
					fmt.Println(err)
				}
			}
		}

		if strings.Contains(user_input, "get") {
			// get {sdfsname} {localname}
			if len(args) == 3 {
				err := Get(args[1], args[2])
				if err != nil {
					fmt.Println(err)
				}
			}
		}

		if strings.Contains(user_input, "delete") {
			if len(args) == 2 {
				err := Delete(args[1])
				if err != nil {
					fmt.Println(err)
				}
			}
		}

		if args[0] == "maple" {
			if len(args) == 5 {
				num_maples, err := strconv.Atoi(args[2])
				if err != nil {
					fmt.Println("Error: Not a valid number of num_maples")
				} else {
					err := StartMaple(args[1], num_maples, args[3], args[4])
					if err != nil {
						fmt.Println("Error running maple:", err)
					}

					if err == nil {
						fmt.Println("!!!Success!!! Maple ran to completion.")
					}
				}
			}
		}

		if args[0] == "juice" {
			if len(args) == 6 {
				num_juices, err := strconv.Atoi(args[2])
				should_del_files := strings.Contains(args[5], "1")
				if err != nil {
					fmt.Println("Error: Not a valid number of num_juices")
				} else {
					err := StartJuice(args[1], num_juices, args[3], args[4], should_del_files)
					if err != nil {
						fmt.Println("Error running juice:", err)
					}

					if err == nil {
						fmt.Println("!!!Success!!! Juice ran to completion.")
					}
				}
			}
		}

		if user_input == "lsmembers" {
			ShowMembers()
		}

		num_tests := 100

		if user_input == "stressput" {
			StressTestPut(num_tests)
		}

		if user_input == "stressget" {
			StressTestGet(num_tests)
		}

		if user_input == "stressdel" || user_input == "stressdelete" {
			StressTestDelete(num_tests)
		}

		if user_input == "stresstest" {
			StressTest(num_tests)
		}

		if user_input == "tokentest" {
			filename_md5 := fmt.Sprintf("%x", md5.Sum([]byte("testfile")))
			token, _ := RequestToken(filename_md5, "W")
			fmt.Println(token)
		}

		if strings.Contains(user_input, "ls") {
			// Show where a given file is stored
			if len(args) == 2 {
				filename_md5 := fmt.Sprintf("%x", md5.Sum([]byte(args[1])))
				replicas := GetNodesWithFile(filename_md5)
				if len(replicas) != 0 {
					fmt.Println(args[1], "is stored at:")
					for _, replica_ip := range replicas {
						fmt.Println(replica_ip)
					}
				} else {
					fmt.Println("No such file")
				}
			}
		}

		// Test Append
		if user_input == "appendtest" {
			AppendTest()
		}

		if strings.Contains(user_input, "find") {
			// Find files with a prefix match
			prefix_to_match := ""
			if len(args) == 2 {
				prefix_to_match = args[1]
			}
			matching_files := GetAllFiles(prefix_to_match)

			for _, name := range matching_files {
				fmt.Println(name)
			}
		}

		if user_input == "store" {
			// Show all files on this node
			my_files.RLock()
			hash_to_names.RLock()
			for key, _ := range my_files.m {
				fmt.Println(hash_to_names.m[key])
			}
			hash_to_names.RUnlock()
			my_files.RUnlock()
		}

		if user_input == "show tokens" {
			ShowTokens()
		}

		if user_input == "show files" {
			my_files.RLock()

			for key, val := range my_files.m {
				fmt.Println(key, " -> ", val)
			}

			my_files.RUnlock()
		}

		if user_input == "self" {
			fmt.Println("Node UUID:", my_unique_id)
			fmt.Println("Node IP:", my_ip_addr)
			fmt.Println("Node hostname:", my_hostname)
		}

		if (user_input == "join" && !ACTIVE) {
			ACTIVE = true

			// Step 3: Send message to the introducer (if we aren't them)
			if (*isIntroducer != "localhost") {
				// Set current mode to X so that the receiver doesn't listen to what our mode is
				CURRENT_MODE = 'X'
				master_id = "x"
				Introduce(*isIntroducer)
			}
			fmt.Println("Joined!")
		}

		if ACTIVE {
			if user_input == "leave" {
				MembershipLeave()
				fmt.Println("Left!")
			}

			if user_input == "mode set gossip" || user_input == "mode set g" {
				SetNetworkMode('G')
			}

			if user_input == "mode set all-to-all" || user_input == "mode set a" {
				SetNetworkMode('A')
			}

			if user_input == "mode" || user_input == "mode get" {
				switch (CURRENT_MODE) {
				case 'G':
					fmt.Println("Gossip")
				case 'A':
					fmt.Println("All to All")
				}
			}
		}
	}
}

/*
 * JoinMembershipSystem
 *
 * Basically perform everything mp1 main to join the network does but automatically
 */
func JoinMembershipSystem (introducer string) {
	if (introducer == "localhost") {
		fmt.Println("Running as introducer...");
	} else {
		fmt.Println("Introducer is", introducer);
	}

	// Step 0: Setup global state
	GetNetworkInfo()

	// Step 1: Setup local node membership value
	SetupSelf()

	// Step 2: Launch server and heartbeat loop
	go MembershipServer()
	go HeartbeatLoop()

	// Step 3: Send message to the introducer (if we aren't them)
	ACTIVE = true
	if (introducer != "localhost") {
		// Set current mode to X so that the receiver doesn't listen to what our mode is
		CURRENT_MODE = 'X'

		// @TODO set master IP to 'X' as well so that receivers don't listen to what we think master is
		master_id = "x"

		// Say hello to the network's topology
		Introduce(introducer)
	}
}

/*
 * AppendTest
 *
 * Test appending to a file
 */
func AppendTest() {
	err := Append("testfile", []byte("One more thing: " + my_ip_addr + "\n"))
	if err != nil {
		fmt.Println("Error in append")
		fmt.Println(err)
	} else {
		Get("testfile", "testfile.txt")
		out_data, _ := exec.Command("tail", "testfile.txt").Output()
		fmt.Println(string(out_data))
	}
}
