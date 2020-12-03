#!/usr/bin/env python3
import sys

def main():
    if len(sys.argv) != 2:
        print("Usage: map1.py input_file")
        exit(-1)

    with open(sys.argv[1], 'r') as file_f:
        for line_raw in file_f.readlines():
            line = line_raw.replace("\n", "")
            votes = line.split(' ')
            m = 3
            for i in range(1, m):
                for j in range(i+1, m+1):
                    if votes[i-1] < votes[j-1]:
                        print(f"([{votes[i-1]}:{votes[j-1]}],1)")
                    else:
                        print(f"([{votes[j-1]}:{votes[i-1]}],0)")

if __name__ == "__main__":
    main()
