#!/bin/bash

# 1 MB
dd if=/dev/urandom of=file1.txt bs=1M count=1

# 10 MB
dd if=/dev/urandom of=file2.txt bs=1M count=10

# 100 MB
dd if=/dev/urandom of=file3.txt bs=1M count=100

# 500 MB
dd if=/dev/urandom of=file4.txt bs=1M count=500

# 1GB
dd if=/dev/urandom of=file5.txt bs=1M count=1000
