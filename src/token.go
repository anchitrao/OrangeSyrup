package main

/*
 * Token
 *
 * A token is a read/ write privilege primative granted by the master
 * to a client. A client must release their token after confirming all
 * operations were successful.
 */

import (
	"fmt"
	"sync"
	"net/rpc"
	"math/rand"
)

// Token state:
// Tokens maps SDFS name hash -> lock
var tokens = struct {
	sync.RWMutex

	// SDFS name hash -> token list
	m map[string][]TokenRequest
}{m: make(map[string][]TokenRequest)}

/*
 * Network synchronization object. Not used as an object anywhere- just used for RPCs
 */
type Token struct { }

/*
 * A token request, used by a node to request a kind of token
 *
 * Nodes can in theory request a token for other nodes by filling fields in here appropriately.
 */
type TokenRequest struct {
	// Node IP address
	IP string `json:"addr"`

	// Filename
	Filename string `json:"filename"`

	// Kind ("R" / "r" or "W" / "w")
	Kind string `json:"tokenkind"`
}

/*
 * A token reply
 *
 * This grants the user a read or write permission for a file in SDFS
 */
type TokenReply struct {
	// IP address this token is granted to
	IP string `json:"granted"`

	// SDFS Name (hashed) allowed to write/ read from
	Filename string `json:"filename"`

	// Permission granted ("R" / "r", "W" / "w", or "X" / "x")
	// R = read, W = write, X = denied
	Kind string `json:"tokenkind"`

	// Replicas allowed to write to / read from
	Replicas []string `json:"replicas"`

	// A list of files found at the node (only used by GetAllFiles)
	AllFiles []string `json:"all_files"`
}

/*
 * Request permission to maple or juice
 */
type MapleJuiceRequest struct {
	IP string `json:"ip"`
}

/*
 * MapleJuiceResponse
 */
type MapleJuiceResponse struct {
	Success bool `json:"allowed"`
}

/*
 * ConnectTokenRPC
 *
 * Returns a TCP connection to a remote token RPC listener (master)
 */
func ConnectTokenRPC (server string) (*rpc.Client, error) {
	client, err := rpc.Dial("tcp", server + ":7070")

	if err != nil {
		fmt.Print(server, " not responding (ConnectTokenRPC)\n")
	}

	return client, err
}

/*
 * ShowTokens
 *
 * Print all currently allocated tokens to console.
 */
func ShowTokens () {
	tokens.Lock()
	for fileid, tokenlist := range tokens.m {
		fmt.Println(fileid,":")
		for idx, token := range tokenlist {
			fmt.Println(idx, " -> ", token)
		}
	}
	tokens.Unlock()
}

/******************
 *  RPC Wrappers  *
 ******************/

/*******************
 *   Client RPCs   *
 *  Talk to master *
 *   on Port 7070  *
 *******************/

/*
 * RequestToken
 *
 * fileid - SDFS filename (hashed)
 * kind- "R" for read or "W" for write
 *
 * Returns a TokenReply
 */
func RequestToken (fileid string, kind string) (TokenReply, error) {
	var reply TokenReply
	reply.Kind = "X"

	connection, err := ConnectTokenRPC(master_id)
	if err != nil {
		return reply, err
	}
	defer connection.Close()

	request := TokenRequest{
		IP: my_ip_addr,
		Filename: fileid,
		Kind: kind,
	}
	err = connection.Call("Token.RequestToken", request, &reply)

	return reply, err
}

/*
 * ReleaseToken
 *
 * Release a token. Returns true on OK, false on failure.
 */
func ReleaseToken (token TokenReply) (bool, error) {
	var reply_ok bool
	reply_ok = false

	connection, err := ConnectTokenRPC(master_id)
	if err != nil {
		return reply_ok, err
	}
	defer connection.Close()

	err = connection.Call("Token.ReleaseToken", token, &reply_ok)
	return reply_ok, err
}

/*
 * GetNodesWithFile
 *
 * Returns a list of all replica IPs of a file in the system
 */
func GetNodesWithFile (fileid string) []string {
	connection, err := ConnectTokenRPC(master_id)
	if err != nil {
		return make([]string, 0)
	}
	defer connection.Close()

	var reply TokenReply
	reply.Replicas = make([]string, 0)
	request := TokenRequest{
		IP: my_ip_addr,
		Filename: fileid,
		Kind: "X",
	}
	err = connection.Call("Token.GetNodesWithFile", request, &reply)

	if err != nil {
		return make([]string, 0)
	}

	return reply.Replicas
}

/*
 * GetAllFiles
 *
 * prefix- a prefix that filenames must match. This can be "" for all files.
 *
 * Returns a list of every single file (in SDFS name) in the system matching prefix.
 */
func GetAllFiles (prefix string) []string {
	connection, err := ConnectTokenRPC(master_id)
	if err != nil {
		return make([]string, 0)
	}
	defer connection.Close()

	var reply TokenReply
	reply.AllFiles = make([]string, 0)
	request := TokenRequest{
		IP: my_ip_addr,
		Filename: prefix,
		Kind: "X",
	}
	err = connection.Call("Token.GetAllFiles", request, &reply)

	if err != nil {
		return make([]string, 0)
	}

	return reply.AllFiles
}

func RequestMapleJuicePerm (ip string) bool {
	connection, err := ConnectTokenRPC(master_id)
	if err != nil {
		return false
	}
	defer connection.Close()

	var reply MapleJuiceResponse
	reply.Success = false
	request := MapleJuiceRequest{ip}
	err = connection.Call("Token.RequestMapleJuicePerm", request, &reply)

	if err != nil {
		return false
	}

	return reply.Success
}

func ReleaseMapleJuicePerm (ip string) bool {
	connection, err := ConnectTokenRPC(master_id)
	if err != nil {
		return false
	}
	defer connection.Close()

	var reply MapleJuiceResponse
	reply.Success = false
	request := MapleJuiceRequest{ip}
	err = connection.Call("Token.ReleaseMapleJuicePerm", request, &reply)

	if err != nil {
		return false
	}

	return reply.Success
}

/******************
 *  RPC Handlers  *
 ******************/

/*
 * Token::RequestToken
 *
 * Request a token
 *
 * Node -> Master
 * RPC Handler that runs on master
 *
 * !!! CAN LOCK members AND tokens !!!
 * Always lock tokens before members
 */
func (t *Token) RequestToken(req TokenRequest, reply *TokenReply) error {
	//fmt.Println(req.IP, "is requesting a", req.Kind, "token for", req.Filename)

	members.Lock()
	defer members.Unlock()

	tokens.Lock()
	defer tokens.Unlock()

	if _, present := tokens.m[req.Filename]; present {
		// Token already in map
	} else {
		// Insert token to our list
		tokens.m[req.Filename] = make([]TokenRequest, 0)
	}

	// Check if we can insert it
	var denied = false
	for _, other_token := range tokens.m[req.Filename] {
		// Deny if any other token is writing, or if we want to write and some other token exists
		if other_token.Kind == "W" || req.Kind == "W" {
			denied = true
			break
		}
	}

	if denied {
		reply.IP = req.IP
		reply.Filename = req.Filename
		reply.Kind = "X"
		reply.Replicas = make([]string, 0)

		// Don't need to unlock tokens- we deferred it :)
		return nil
	}

	tokens.m[req.Filename] = append(tokens.m[req.Filename], req)

	// Choose N replicas
	// @TODO: Pick N_REPLICAS replicas
	// For now just store everything on master
	reply.IP = req.IP
	reply.Filename = req.Filename
	reply.Kind = req.Kind

	reply.Replicas = make([]string, 0)

	// All replicas in the system already
	existing_replicas := make([]string, 0)
	for _, member := range members.m {
		if HasFile(member.IP, req.Filename) {
			existing_replicas = append(existing_replicas, member.IP)
		}
	}

	if req.Kind == "W" || req.Kind == "w" {
		randoms_to_choose := N_REPLICAS
		if len(existing_replicas) != 0 {
			// Some replicas exist
			for _, replica := range existing_replicas {
				reply.Replicas = append(reply.Replicas, replica)
			}
			randoms_to_choose = N_REPLICAS - len(existing_replicas)
		}

		// Pick randoms_to_choose random nodes and send to them
		replicas_keys := make([]string, 0)
		for member_key, member_val := range members.m {
			// Only make this key available if it is not already in the replicas list
			should_insert := true
			for _, existing_replica_val := range reply.Replicas {
				if member_val.IP == existing_replica_val {
					should_insert = false
				}
			}

			// Always insert self
			// We insert self later so don't insert now
			if member_val.IP == req.IP {
				should_insert = false
			}

			if should_insert {
				replicas_keys = append(replicas_keys, member_key)
			}
		}

		if randoms_to_choose > 0 {
			// Always choose self
			reply.Replicas = append(reply.Replicas, req.IP)
			randoms_to_choose -= 1
		}

		nodes_talked_to := 0
		for _, random_key := range rand.Perm(len(replicas_keys)) {
			if nodes_talked_to >= randoms_to_choose {
				break
			}
			reply.Replicas = append(reply.Replicas, members.m[replicas_keys[random_key]].IP)
			nodes_talked_to++
		}
	}

	if req.Kind == "R" || req.Kind == "r" {
		if len(existing_replicas) > 0 {
			// Make sure not to select the requestor as the replica we reply with
			for i := 0; i < len(existing_replicas); i++ {
				if req.IP != existing_replicas[i] {
					reply.Replicas = []string{existing_replicas[i]}
				}
			}
		}
	}

	// Remove any duplicates from reply replicas
	replicas_already_present := make(map[string]bool)
	final_replicas := make([]string, 0)
	for _, val := range reply.Replicas {
		if _, present := replicas_already_present[val]; !present {
			replicas_already_present[val] = true
			final_replicas = append(final_replicas, val)
		}
	}
	reply.Replicas = final_replicas

	// Don't need to unlock tokens- we deferred it :)
	return nil
}

/*
 * Token::ReleaseToken
 *
 * Release a previously assigned token
 * Node -> Master
 */
func (t *Token) ReleaseToken(req TokenReply, ok *bool) error {
	//fmt.Println(req.IP, "is releasing a", req.Kind, "token for", req.Filename)
	tokens.Lock()
	RemoveTokenFromIP(req.Filename, req.IP, req.Kind)
	tokens.Unlock()
	*ok = true

	return nil
}

/*
 * RemoveTokenFromIP
 *
 * Helper method for ReleaseToken
 * Also called by membership when a failed node is detected.
 *
 * Assumption- tokens lock is held.
 */
func RemoveTokenFromIP (filename string, ip string, kind string) {
	// Delete token from the list if it exists
	if _, present := tokens.m[filename]; present {
		for idx, val := range tokens.m[filename] {
			if val.IP == ip && val.Kind == kind {
				// Remove from the slice by appending before and after it
				tokens.m[filename] = append(tokens.m[filename][:idx], tokens.m[filename][idx+1:]...)
			}
		}

		// Delete the entire list if empty
		if len(tokens.m[filename]) == 0 {
			delete(tokens.m, filename)
		}
	}
}

/*
 * RemoveAllTokensFromIP
 *
 * Deletes all tokens for an IP.
 * Aquires the tokens lock
 */
func RemoveAllTokensFromIP (ip string) {
	tokens.Lock()
	for filename, token_array := range tokens.m {
		for idx, token := range token_array {
			if token.IP == ip {
				// Remove from the slice by appending before and after it
				tokens.m[filename] = append(tokens.m[filename][:idx], tokens.m[filename][idx+1:]...)
			}
		}

		// Delete the entire list if empty
		if len(tokens.m[filename]) == 0 {
			delete(tokens.m, filename)
		}
	}
	tokens.Unlock()
}

/*
 * Token::GetNodesWithFile
 *
 * Returns a list of IPs that have replicas of a file
 */
func (t *Token) GetNodesWithFile (req TokenRequest, reply *TokenReply) error {
	members.RLock()
	existing_replicas := make([]string, 0)
	for _, member := range members.m {
		if HasFile(member.IP, req.Filename) {
			existing_replicas = append(existing_replicas, member.IP)
		}
	}
	members.RUnlock()

	reply.Replicas = existing_replicas
	return nil
}

/*
 * Token::GetAllFiles
 *
 * Called on master. Returns all files in the system.
 */
func (t *Token) GetAllFiles (req TokenRequest, reply *TokenReply) error {
	all_files := make([]string, 0)
	members.RLock()
	for _, member := range members.m {
		// @TODO if this is being a pain add an if statement here
		// and only call GetAllFilesAtNode if member.IP != my_ip (master IP)
		files_from_node := GetAllFilesAtNode(member.IP, req.Filename)
		all_files = append(all_files, files_from_node...)
	}
	members.RUnlock()

	// Remove duplicates
	keys := make(map[string]bool)
	final_list := make([]string, 0)
	for _, val := range all_files {
		if _, present := keys[val]; !present {
			keys[val] = true
			final_list = append(final_list, val)
		}
	}

	reply.AllFiles = final_list
	return nil
}

var maplejuiceperm = struct {
	sync.RWMutex

	// Who is using? blank if none
	IP string
}{IP: ""}

func (t *Token) RequestMapleJuicePerm (req MapleJuiceRequest, reply *MapleJuiceResponse) error {
	maplejuiceperm.Lock()
	defer maplejuiceperm.Unlock()

	reply.Success = false
	if maplejuiceperm.IP == "" {
		maplejuiceperm.IP = req.IP
		reply.Success = true
	}

	return nil
}

func (t *Token) ReleaseMapleJuicePerm (req MapleJuiceRequest, reply *MapleJuiceResponse) error {
	maplejuiceperm.Lock()
	defer maplejuiceperm.Unlock()

	reply.Success = false
	if maplejuiceperm.IP == req.IP {
		maplejuiceperm.IP = ""
		reply.Success = true
	}

	return nil
}
