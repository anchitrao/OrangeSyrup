package main

import (
	"fmt"
	"bufio"
	"strings"
	"flag"
	"time"
	"math/rand"
	"os"
)

/*
 * Program global state
 */
// Our local node info (useful to keep around)
var my_ip_addr = ""
var my_hostname = ""
var my_unique_id = ""

// Constants
// TFAIL TCLEANUP are in seconds
var TFAIL = int64(2)
var TCLEANUP = TFAIL

// How many nodes to gossip to when in gossip mode?
// Set to 4 because we are expecting up to 3 failures
var GOSSIP_N = 4

// Set to true when we join, set to false right after
// When this is false, server should do nothing
var ACTIVE = false

// Background logging state vars
var BG_LOGGING = false
var BG_LOG_BEGIN time.Time
var BG_LOG_END time.Time
var BG_LOG_COUNT = 0 // # bytes in/ out, excluding modeset packets leaving

// Log to a file
var FILE_LOG *os.File
var FILE_LOG_ENABLE = false

// How many replicas to have of a file?
const N_REPLICAS = 4

/*
 * main entrypoint from mp1
 */
func main_mp1 () {
	// Seed randomizer:
	rand.Seed(time.Now().UnixNano())

	// Switch between introducer and regular node
	isIntroducer := flag.String("i", "localhost", "Specify the introducer node address")
	autoJoin := flag.Bool("j", false, "Automatically join the server")
	flag.Parse()

	if (*isIntroducer == "localhost") {
		fmt.Println("Running as introducer...");
	} else {
		fmt.Println("Introducer is", *isIntroducer);
	}

	// Step 0: Setup global state
	GetNetworkInfo()

	// Step 1: Setup local node membership value
	SetupSelf()

	// Step 2: Launch server and heartbeat loop
	go MembershipServer()
	go HeartbeatLoop()

	// Wait a bit for everything to kick off
	time.Sleep(time.Millisecond * 100)

	// Step 4: Handle any user input
	io_reader := bufio.NewReader(os.Stdin)

	// Set ACTIVE to true if we are the introducer (starts out joined)
	ACTIVE = (*isIntroducer == "localhost")
	have_we_ever_joined := ACTIVE

	for {
		fmt.Print("> ")
		user_input, _ := io_reader.ReadString('\n')
		user_input = strings.ReplaceAll(user_input, "\n", "")

		if (user_input == "join" && !ACTIVE) || (!have_we_ever_joined && *autoJoin) {
			ACTIVE = true
			have_we_ever_joined = true

			// Step 3: Send message to the introducer (if we aren't them)
			if (*isIntroducer != "localhost") {
				// Set current mode to X so that the receiver doesn't listen to what our mode is
				CURRENT_MODE = 'X'
				Introduce(*isIntroducer)
			}
			fmt.Println("Joined!")
		}

		if ACTIVE {
			// Mode set commands
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

			// Show membership list
			if user_input == "ls" {
				ShowMembers()
			}

			if user_input == "self" {
				fmt.Println("Node UUID:", my_unique_id)
				fmt.Println("Node IP:", my_ip_addr)
				fmt.Println("Node hostname:", my_hostname)
			}

			if user_input == "leave" {
				MembershipLeave()
				fmt.Println("Left!")
			}

			// Enable / disable background noise logging
			if user_input == "bglog enable" {
				BG_LOG_COUNT = 0
				BG_LOGGING = true
				BG_LOG_BEGIN = time.Now()
			}
			if user_input == "bglog disable" {
				BG_LOGGING = false
				BG_LOG_END = time.Now()

				fmt.Println("Bytes: ", BG_LOG_COUNT)
				fmt.Println("Time:", BG_LOG_END.Sub(BG_LOG_BEGIN))
			}

			if user_input == "file log enable" {
				FILE_LOG_ENABLE = true
				FILE_LOG, _ = os.Create("/tmp/ece428_mp1_log.txt")
				defer FILE_LOG.Close()

				FILE_LOG.WriteString(time.Now().String() + ": " + "Logging enabled for node " + my_unique_id + " (IP: " + my_ip_addr + ")\n")
			}
		}
	}
}