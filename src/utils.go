package main

import (
	"os"
	"os/exec"
	"strings"
	"bufio"
	"fmt"
)

/*
 * GetFileHash
 *
 * Returns the hash of a given path
 */
func GetFileHash (filepath string) (string, error) {
	// Check that file exists
	file, err := os.Open(filepath)

	// Couldn't find file
	if err != nil {
		fmt.Println("Error in GetFileHash")
		return "", err
	}
	file.Close()

	// Get its hash
	// @TODO: Look into md5sum hash checking modes
	// !! THIS IS NOT SECURE !!
	md5sum_cmd := exec.Command("md5sum", filepath)
	md5sum_output, err := md5sum_cmd.Output()

	md5sum := string(md5sum_output)
	md5sum = strings.Split(md5sum, " ")[0]

	return md5sum, nil
}

/*
 * CountLinesInFile
 *
 * Returns the number of newline chars in a file.
 */
func CountLinesInFile (filepath string) (int, error) {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println("Error counting lines")
		fmt.Println(err)
		return 0, err
	}

	num_lines := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		num_lines+=1
	}

	return num_lines, nil
}
