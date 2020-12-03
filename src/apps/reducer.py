#!/usr/bin/env python3
import sys

def main():
    if len(sys.argv) != 2:
        print("Usage: reducer.py input_file")
        exit(-1)

    with open(sys.argv[1], 'r') as file_f:
        wordcount = 0
        for line_raw in file_f.readlines():
            line = line_raw.replace("\n", "")
            key = line.split("(")[1].split(",")[0]
            val = line.split(",")[1].split(")")[0]
            if val.isnumeric():
                wordcount += int(val)

        print(f"{wordcount}")

if __name__ == "__main__":
    main()
