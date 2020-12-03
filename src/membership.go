package main

/*
 * Joseph Ravichandran
 *
 * ECE 428 Distributed Systems
 * MP 1: Membership Lists
 */

import (
	// For printing to stdout:
	"fmt"

	// Networking:
	"net"

	// JSON:
	"encoding/json"

	"log"
	"strings"

	// For mutexes on maps
	"sync"

	// Unique ID generation:
	"os"
	"os/exec"
	"crypto/md5"
	"time"
	"math/rand"

	"path"
)

/***********************
 *                     *
 *     Server State    *
 *                     *
 ***********************/
type Member struct {
	// DONT FORGET TO CAPITALIZE THESE OR ELSE THEY WONT SHOW UP ANYWHERE!!!
	// IP address of a node
	IP string `json:"addr"`

	// Latest heartbeat we have heard about in Unix epoch time (time.Now().Unix())
	Latest_heartbeat int64 `json:"time"`
}

// Members maps IP string -> initial timestamp and last ping timestamp
//var members = make (map[string]Member)
// Maps are not concurrent-safe so we need to wrap it with a mutex for rw synchronization
var members = struct {
	sync.RWMutex
	m map[string]Member
}{m: make(map[string]Member)}

// List of strings associated with failed nodes
// Using a map to get hashtable data structure
// maps IDs -> time of failure detection
var failed_list = make(map[string]int64)

// Which transmission mode are we in? Gossip ('G') or All-to-all ('A')?
var CURRENT_MODE byte = 'G'
var last_modeset_packet_time time.Time
var last_masterset_packet_time time.Time

/***********************
 *                     *
 *     Server Code     *
 *                     *
 ***********************/

/*
 * MembershipServer
 *
 * Launch a listener on port 6060 that handles heartbeats from other nodes.
 */
func MembershipServer() {
	// Setup UDP Listener:
	addr, err := net.ResolveUDPAddr("udp", ":6060")
	if err != nil {
		log.Fatal(err)
	}

	inbound, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Listening on", addr)

	// Defer = this will run when the function quits
	defer inbound.Close()

	// In go, while loops are for loops
	for {
		// Blocking read from UDP socket
		buffer := make([]byte, 2048)
		n_bytes, addr, err := inbound.ReadFromUDP(buffer)

		if BG_LOGGING {
			BG_LOG_COUNT += n_bytes
		}

		if err != nil {
			fmt.Println(addr, err)
		} else{
			// Launch goroutine to handle this packet
			go MembershipHandleReply(n_bytes, addr, inbound, buffer)
		}
	}
}

/*
 * MembershipHandleReply
 *
 * Goroutine responsible for handling a reply from a server and updating server state.
 *
 * n_bytes: length of buffer
 * addr: UDPAddr responsible for making this connection (who sent this packet?)
 * inbound: UDPConn associated with our UDP listener (we use this to send a reply)
 * buffer: buffer containing data from the packet
 */
func MembershipHandleReply (n_bytes int, addr *net.UDPAddr, inbound *net.UDPConn, buffer []byte) {
	command_mode := buffer[0]
	
	if ACTIVE == false {
		// If we're inactive, it should be as if we aren't here (send no data out)
		return
	}

	// Modeset:
	// We allow mode setting packets while inactive because its something passive that keeps us up
	// to date with the network, and allows for immediate mode switching
	if (command_mode == 'M') {
		last_modeset_packet_time = time.Now()
		if (buffer[1] == 'G') {
			fmt.Println("Setting mode to Gossip")
			CURRENT_MODE = 'G'
		}
		if (buffer[1] == 'A') {
			fmt.Println("Setting mode to All-to-All")
			CURRENT_MODE = 'A'
		}
		inbound.WriteToUDP([]byte("ACK"), addr)
	}

	// Masterset:
	// Set the master to this new value immediately
	// This is sent when the master changes and an election is held.
	if (command_mode == 'S') {
		last_masterset_packet_time = time.Now()

		var master_dict map[string]string
		json.Unmarshal(buffer[1:n_bytes], &master_dict)

		new_master := master_dict["m"]

		if new_master != "x" {
			master_id = new_master
		}

		inbound.WriteToUDP([]byte("ACK"), addr)
	}

	// Master suggest:
	// Gossip who I think the master currently is
	if (command_mode == 's') {
		if (time.Now().After(last_masterset_packet_time.Add(time.Second))) {
			var master_dict map[string]string
			json.Unmarshal(buffer[1:n_bytes], &master_dict)

			new_master := master_dict["m"]

			if new_master != "x" {
				master_id = new_master
			}
		}
	}

	// Regular update:
	if (command_mode == 'U') {
		// Read whatever mode the sender is in and set to that
		// (Allows uninitiated nodes to "pick up" on the right behavior)
		// No need to worry about cycles due to the presence of 'M' packets
		// Still, use last mode set time as a heuristic of whether to believe this or not
		// (If we just got a modeset packet, value that over this)
		if (time.Now().After(last_modeset_packet_time.Add(time.Second))) {
			// Only set to 'G' or 'A' (if we receive an 'X' or something ignore it)
			if (buffer[1] == 'G') {
				CURRENT_MODE = 'G'
			}
			if (buffer[1] == 'A') {
				CURRENT_MODE = 'A'
			}
		}

		// Unmarshal JSON data and compare the remote list to our list
		// JSON data starts at byte index 2 (bytes 0 and 1 represent packet kind and transmission mode)
		var remote_members map[string]Member
		json.Unmarshal(buffer[2:n_bytes], &remote_members)

		members.Lock()
		for identifier, remote_member := range remote_members {
			if local_member, present := members.m[identifier]; present {
				// We have a local member with the same identifier
				if (members.m[identifier].Latest_heartbeat < remote_members[identifier].Latest_heartbeat) {
					//fmt.Println("Updating timestamp for", local_member.IP)
					// Update the latest time of this node
					members.m[identifier] = Member{IP: local_member.IP, Latest_heartbeat: remote_member.Latest_heartbeat}
				}
			} else {
				// This remote ID isn't in our membership list
				// First check if it is in the fail list
				if _, present := failed_list[identifier]; !present{
					//fmt.Println("Adding a new member")
					members.m[identifier] = remote_member

					if FILE_LOG_ENABLE {
						FILE_LOG.WriteString(time.Now().String() + ": " + "Adding remote user with ID " + identifier + " (IP: " + remote_member.IP + ")\n")
					}
				}
			}
		}
		members.Unlock()
	}
}

/***********************
 *                     *
 *     Client Code     *
 *                     *
 ***********************/

/*
 * HeartbeatLoop
 *
 * Continuously ping all nodes in the heartbeat according to the current protocol.
 */
func HeartbeatLoop () {
	for {
		// Update members with our latest timestamp for our node:
		members.Lock()
		cur_val := members.m[my_unique_id]
		members.m[my_unique_id] = Member{IP: cur_val.IP, Latest_heartbeat: time.Now().Unix()}

		// Only transmit if we are active
		if ACTIVE {
			// All-To-All mode
			if (CURRENT_MODE == 'A') {
				// Send to all nodes but us
				for identifier, member := range members.m {
					if (identifier != my_unique_id) {
						// Broadcast to the others
						// Could launch this as a goroutine but that may introduce contention for the lock
						SendHeartbeat(member.IP)
						GossipMaster(member.IP)
					}
				}
			}

			// Gossip mode
			if (CURRENT_MODE == 'G') {
				// Pick up to GOSSIP_N random nodes and send to them
				members_keys := make([]string, 0, len(members.m))
				for key := range members.m {
					if (key != my_unique_id) {
						members_keys = append(members_keys, key)
					}
				}

				nodes_talked_to := 0
				for _, random_key := range rand.Perm(len(members_keys)) {
					if nodes_talked_to > GOSSIP_N {
						break
					}
					SendHeartbeat(members.m[members_keys[random_key]].IP)
					GossipMaster(members.m[members_keys[random_key]].IP)
					nodes_talked_to++
				}
			}

			// Check for failed nodes
			current_time := time.Now().Unix()
			for identifier, member := range members.m {
				if (member.Latest_heartbeat < current_time - TFAIL) {
					// This member has officially failed and will be removed
					// Crash-stop failure model
					//fmt.Println("Node", identifier, "(", member.IP, ")", "has failed (last ping:", member.Latest_heartbeat, ", time is:", time.Now().Unix())
					if _, present := failed_list[identifier]; !present {
						// Add to failed list
						if FILE_LOG_ENABLE {
							FILE_LOG.WriteString(time.Now().String() + ": " + "Node " + identifier + " (IP: " + members.m[identifier].IP + ") failed or left\n")
						}

						// Remove any tokens belonging to this node
						RemoveAllTokensFromIP(members.m[identifier].IP)

						// If master is about to fail, elect a new one
						need_to_elect := false
						if members.m[identifier].IP == master_id {
							need_to_elect = true
						}

						// @TODO: Check if any partner replicas failed
						// A list of all file hashes we share with the failing node
						// AND we are the highest IP in the replica region
						stuff_to_replicate := make([]string, 0)
						my_files.RLock()
						for hash, replicas := range my_files.m {
							leader_ip := my_ip_addr
							potentially_insert := false
							for _, replica_ip := range replicas {
								// find who the highest ip is with a replica
								if replica_ip > leader_ip && replica_ip != member.IP {
									leader_ip = replica_ip
								}
								
								// The failed IP is in the list of replicas for this file hash
								if replica_ip == member.IP {
									potentially_insert = true
								}
							}
							
							// if we are the highest then add hash to arry to be inserted later
							if leader_ip == my_ip_addr && potentially_insert {
								stuff_to_replicate = append(stuff_to_replicate, hash)
								fmt.Println("Detected failure at file ID", hash)
							}
						}
						my_files.RUnlock()

						failed_list[identifier] = current_time
						delete(members.m, identifier)

						// Do this after deleting because otherwise we timeout trying to send a master config
						// packet to the failed master node! (ironic)
						if need_to_elect {
							next_master := my_ip_addr
							for _, member_val := range members.m {
								if next_master < member_val.IP {
									next_master = member_val.IP
								}
							}
							master_id = next_master
							if master_id == my_ip_addr {
								SetMaster(my_ip_addr)
							}
						}

						// replicates files where necessary
						for _, hash := range stuff_to_replicate {
							go func() {
								hash_to_names.RLock()
								sdfsname := hash_to_names.m[hash]
								hash_to_names.RUnlock()
								fmt.Println("Replicating file", sdfsname)

								time.Sleep(time.Second * 2)

								// @TODO: Maybe do another check here or something
								Put(path.Join("files", hash), sdfsname)
							}()
						}
					}
				}
			}

			// Cleanup all failed nodes
			for identifier, fail_time := range failed_list {
				if fail_time < current_time - TCLEANUP {
					//fmt.Println("Deleting a node")
					// Remove this identifier from the fail list
					delete(failed_list, identifier)
				}
			}
		}

		members.Unlock()
		time.Sleep(time.Millisecond * 250)
	}
}

/*
 * SetMaster
 *
 * Notify the network that a new master has been chosen immediately.
 *
 * !! Assumption- the members lock has already been aquired !!
 */
func SetMaster (new_master_ip string) {
	had_a_problem := false
	for _, member := range members.m {
		// Send to all, including ourselves:
		worked, err := SendMasterConfigPacket(member.IP, new_master_ip)
		if err != nil || worked != true {
			had_a_problem = true
		}
	}
	if (had_a_problem) {
		fmt.Println("[WARNING] Some nodes didn't reply")
	}
}

/*
 * GossipMaster
 *
 * Send a master suggest packet with who we think the master currently is.
 */
func GossipMaster (target string) error {
	remote_node, err := MembershipConnect(target)
	if err != nil {
		// This node is dead!
		return err
	}
	defer remote_node.Close()

	var jsondata []byte
	var master_dict = make(map[string]string)
	master_dict["m"] = master_id
	jsondata, err = json.Marshal(master_dict)

	if err != nil {
		fmt.Println("Error Marshaling master!")
		log.Fatal(err)
	}

	var senddata []byte
	senddata = append(senddata, 's') // s = soft suggest master
	senddata = append(senddata, jsondata...) // Use 3 dots to unpack jsondata

	remote_node.Write(senddata)

	// Don't wait to read a reply, not needed
	return nil
}

/*
 * SendMasterConfigPacket
 *
 * Sends a new master config packet (using command byte of 'S')
 */
func SendMasterConfigPacket (target string, new_master_ip string) (bool, error) {
	remote_conn, err := MembershipConnect(target)
	if err != nil {
		return false, err
	}

	defer remote_conn.Close()

	// S = "masterset" command
	var jsondata []byte
	var master_dict = make(map[string]string)
	master_dict["m"] = new_master_ip
	jsondata, _ = json.Marshal(master_dict)

	databuf := []byte{'S'};
	databuf = append(databuf, jsondata...)

	// Force a reply (within a deadline)
	// Nothing can happen until this completes
	remote_conn.Write(databuf)

	// Deadline of TFAIL seconds
	deadline := time.Now().Add(time.Second * time.Duration(TFAIL))
	remote_conn.SetReadDeadline(deadline)

	// Reply should be 'ACK' or error so 16 bytes is more than enough
	readbuf := make([]byte, 16)
	_, _, err = remote_conn.ReadFrom(readbuf)

	if err != nil {
		return false, err
	}

	if (readbuf[0] != 'A' || readbuf[1] != 'C' || readbuf[2] != 'K') {
		fmt.Println("didnt get 'ack'")
		return false, nil
	}

	return true, nil
}


/*
 * SetNetworkMode
 *
 * Change the network to using either gossip-style ('G') or all-to-all ('A')
 */
func SetNetworkMode (mode byte) {
	members.RLock()
	had_a_problem := false
	for _, member := range members.m {
		// Send to all, including ourselves:
		worked, err := SendNetworkModeConfigPacket(member.IP, mode)
		if err != nil || worked != true {
			had_a_problem = true
		}
	}
	if (had_a_problem) {
		fmt.Println("[WARNING] Some nodes didn't reply")
	}
	fmt.Println("Done setting network mode!")
	members.RUnlock()
}

/*
 * SendNetworkModeConfigPacket
 *
 * Sends a "modeset" command to a given IP address, and waits for a reply.
 *
 * Returns (ok, error), where ok is true if we received an "ACK", and error != nil when there was an error.
 *
 * This will only work correctly at steady state.
 */
func SendNetworkModeConfigPacket (target string, mode byte) (bool, error) {
	remote_conn, err := MembershipConnect(target)
	if err != nil {
		return false, err
	}

	defer remote_conn.Close()

	// This command is just 2 bytes
	// M = "modeset" command
	databuf := []byte{'M', mode};

	// Force a reply (within a deadline)
	// Nothing can happen until this completes
	remote_conn.Write(databuf)

	// Deadline of TFAIL seconds
	deadline := time.Now().Add(time.Second * time.Duration(TFAIL))
	remote_conn.SetReadDeadline(deadline)

	// Reply should be 'ACK' or error so 16 bytes is more than enough
	readbuf := make([]byte, 16)
	_, _, err = remote_conn.ReadFrom(readbuf)

	if err != nil {
		return false, err
	}

	if (readbuf[0] != 'A' || readbuf[1] != 'C' || readbuf[2] != 'K') {
		fmt.Println("didnt get 'ack'")
		return false, nil
	}

	return true, nil
}

/*
 * MembershipConnect
 *
 * Connect to a remote node
 *
 * name: resource identifier for the remote server
 * Returns a UDPConn on success, or error on failure
 */
func MembershipConnect(name string) (*net.UDPConn, error) {
	remote_addr, err := net.ResolveUDPAddr("udp", name + ":6060")
	if err != nil {
		return nil, err
	}

	client, err := net.DialUDP("udp", nil, remote_addr)
	if err != nil {
		fmt.Print("\t\t\t", name, " not responding (MembershipConnect)\n")
		return nil, err
	}

	return client, err
}

/*
 * SendHeartbeat
 *
 * Sends a heartbeat to a target name.
 *
 * Assumption: the caller has aquired a read lock to members
 */
func SendHeartbeat (target string) error {
	remote_node, err := MembershipConnect(target)
	if err != nil {
		// This node is dead!
		return err
	}
	defer remote_node.Close()

	// Marshal our member list
	var jsondata []byte

	// Assume the caller has this lock already! (only called in heartbeat loop and introduce)
	jsondata, err = json.Marshal(members.m)

	if err != nil {
		fmt.Println("Error Marshaling membership list!")
		log.Fatal(err)
	}

	// Append the operation code (hearbeat flavor ?)
	var senddata []byte
	senddata = append(senddata, 'U') // U = regular transmission, updating nodes
	senddata = append(senddata, byte(CURRENT_MODE)) // Add current mode as 2nd byte of packet
	senddata = append(senddata, jsondata...) // Use 3 dots to unpack jsondata

	if BG_LOGGING {
		BG_LOG_COUNT += len(senddata)
	}

	// Send the data to the target node
	remote_node.Write(senddata)

	// Don't wait to read a reply, not needed
	return nil
}

/*
 * GetNetworkInfo
 *
 * Returns system network IP and symbolic hostname.
 */
func GetNetworkInfo () (ip string, hostname string) {
	// Terminal command 'hostname -I' returns the IP address
	ipCmd := exec.Command("hostname", "-I")
	output, err := ipCmd.Output()
	if err != nil {
		fmt.Println("Error, couldn't fetch hostname", err)
	}

	output_str := strings.ReplaceAll(string(output), "\n", "")
	output_str = strings.ReplaceAll(output_str, " ", "")

	hostname_str, _ := os.Hostname()

	my_ip_addr = output_str
	my_hostname = hostname_str

	return output_str, hostname_str
}

/*
 * CalculateNodeID
 *
 * Calculates a node ID by taking the md5 hash of IP, hostname, and current time.
 */
func CalculateNodeID () (id string) {
	// Generate a string of {IP}{symbolic hostname}{current time in NS} ("ID qualifier")
	ip, hname := GetNetworkInfo()
	time_str := fmt.Sprintf("%d", time.Now().UnixNano())
	id_qualifier := ip + hname + time_str

	// Hash the ID qualifier
	id_bytes := []byte(id_qualifier)
	id_int := md5.Sum(id_bytes)

	// Convert hash to string
	id_str := fmt.Sprintf("%x", id_int)
	return id_str
}

/*
 * SetupSelf
 *
 * Create a member node containing myself and add it to my members table.
 */
func SetupSelf() string {
	// Yes, this is redundant, but its here for simplicity and ignoring concurrency issues
	ip, _ := GetNetworkInfo()

	myself_member := Member{IP: ip, Latest_heartbeat: time.Now().Unix()}
	myself_id := CalculateNodeID()

	members.Lock()
	members.m[myself_id] = myself_member
	members.Unlock()

	my_unique_id = myself_id

	return myself_id
}

/*
 * Send a packet introducing ourselves to the introducer.
 *
 * Can't just add the introducer to the members list and start transmitting
 * because we don't know the introducer's UUID.
 */
func Introduce(introducer string) {
	// Verify that the introducer is live:
	conn, err := MembershipConnect(introducer)

	if err != nil {
		fmt.Println(err)
		log.Fatal("Introducer is down! Stopping")
	}

	// UDP so this does nothing:
	conn.Close()

	// Send a heartbeat to the introducer
	SendHeartbeat(introducer)
}

/*
 * ShowMembers
 *
 * List all members currently active.
 */
func ShowMembers () {
	members.Lock()
	for identifier, member := range members.m {
		fmt.Println(identifier, "\t", member.IP, "\t", member.Latest_heartbeat)
	}
	members.Unlock()
}

/*
 * MembershipLeave
 *
 * Leave the network.
 */
func MembershipLeave() {
	ACTIVE = false
	members.Lock()
	// Delete all members
	for identifier := range members.m {
		delete(members.m, identifier)
	}
	members.Unlock()

	// Reset all state with a new time hash (new ID)
	// Should refactor this but its 2 hours before deadline, so...
	last_modeset_packet_time = time.Time{}
	last_masterset_packet_time = time.Time{}
	CURRENT_MODE = 'G'
	my_ip_addr, my_hostname = GetNetworkInfo()
	my_unique_id = SetupSelf()

	if FILE_LOG_ENABLE {
		FILE_LOG.WriteString(time.Now().String() + ": " + "I left\n")
	}
}
