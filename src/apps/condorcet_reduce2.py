#!/usr/bin/env python3
import sys

def main():
    if len(sys.argv) != 2:
        print("Usage: map1.py input_file")
        exit(-1)

    with open(sys.argv[1], 'r') as file_f:
        Carray = {'A':0, 'B':0, 'C':0}
        for line_raw in file_f.readlines():
            line = line_raw.replace("\n", "")
            key = line.split("(")[1].split(",")[0]
            val = line.split(",")[1].split(")")[0]
            candidates = val[1:-1].split(":")
            Carray[candidates[0]] += 1
            if Carray[candidates[0]] == 2:
                print(f"({candidates[0]},'Condorcet winner!')")
                return
        best_candidates = [c for c in Carray if Carray[c] == max(Carray.values())]
        print(f"({':'.join(best_candidates)},'”No Condorcet winner, Highest Condorcet counts')")

if __name__ == "__main__":
    main()
