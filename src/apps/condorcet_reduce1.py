#!/usr/bin/env python3
import sys

def main():
    if len(sys.argv) != 2:
        print("Usage: map1.py input_file")
        exit(-1)

    with open(sys.argv[1], 'r') as file_f:
        num_zero = 0
        num_one = 0
        for line_raw in file_f.readlines():
            line = line_raw.replace("\n", "")
            key = line.split("(")[1].split(",")[0]
            candidates = key[1:-1].split(":")
            val = line.split(",")[1].split(")")[0]
            if val == '1':
                num_one += 1
            else:
                num_zero += 1
        if num_one > num_zero:
            print(f"({candidates[0]},{candidates[1]})")
        else:
            print(f"({candidates[1]},{candidates[0]})")

if __name__ == "__main__":
    main()
