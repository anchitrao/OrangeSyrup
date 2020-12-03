#!/bin/bash
# Run a single container
cd src
docker build -t temp .
docker run --rm -it --volume="`pwd`:/home/proj/src:rw" temp bash
