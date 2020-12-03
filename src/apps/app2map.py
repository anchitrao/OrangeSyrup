#!/usr/bin/env python3
import sys

def main():
    if len(sys.argv) != 2:
        print("Usage: map1.py input_file")
        exit(-1)

    with open(sys.argv[1], 'r') as file_f:
        for line_raw in file_f.readlines():
            line = line_raw.replace("\n", "")
            info = line.split(' ')

            address = info[6]
            typeArm = info[3]
            color = info[4]

            if typeArm == "Mast:Arm" and color == "Black":
                print(f"({info[6]},1)")

if __name__ == "__main__":
    main()
