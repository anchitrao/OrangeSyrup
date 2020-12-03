package main

/*
 * Master
 *
 * Methods specific to the master node
 *
 * This is just RPCs for managing tokens and caching file locations
 */

import (
	"fmt"
	"net/rpc"
	"net"
)

/*
 * MasterRPCServer
 *
 * Listen on port 7070 for RPCs
 */
func MasterRPCServer() error {
	networkState := new(Token)

	// Setup TCP listener:
	addr, err := net.ResolveTCPAddr("tcp", ":7070")

	if err != nil {
		fmt.Println("Error", err)
		return err
	}

	inbound, err := net.ListenTCP("tcp", addr)

	if err != nil {
		fmt.Println("Error", err)
		return err
	}

	maplejuiceperm.Lock()
	maplejuiceperm.IP = ""
	maplejuiceperm.Unlock()

	rpc.Register(networkState)

	// Loop on accepting inputs
	rpc.Accept(inbound)

	return nil
}
