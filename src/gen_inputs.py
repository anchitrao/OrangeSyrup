#!/usr/bin/env python3
import sys
import random

keys = ["A", "B", "C", "D"]

with open(sys.argv[1], 'w') as file:
    for i in range(1000):
        file.write(random.choice(keys))
        file.write("\n")

# N = 100
# with open(sys.argv[1], 'w') as file:
#     for k in keys:
#         for i in range(N):
#             file.write(k)
#             file.write("\n")
