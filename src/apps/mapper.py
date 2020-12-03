#!/usr/bin/env python3
import sys

def main():
    if len(sys.argv) != 2:
        print("Usage: mapper.py input_file")
        exit(-1)

    with open(sys.argv[1], 'r') as file_f:
        for line_raw in file_f.readlines():
            line = line_raw.replace("\n", "")
            for word_raw in line.split(' '):
                word = word_raw.replace(",", "").replace("(", "").replace(")", "").replace("/", "").replace("\\","").replace(".","")
                print(f"({word},1)")

if __name__ == "__main__":
    main()
