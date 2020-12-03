# This could be Ubuntu:18.04 for non-go projects
FROM golang:1.15

# A build argument, this is the name of a node
ARG node_name_prefix

# Install useful utilities:
RUN apt-get update && apt-get install -y iputils-ping netcat vim tmux python3 host

# This is where docker-compose will mount our files inside the container
WORKDIR /home/proj/src

RUN echo "set -g mouse on" >> ~/.tmux.conf

# Copy sources into src and build
COPY . /home/proj/src
RUN cd /home/proj/src

# Find any instances of $NODE_NAME_PREFIX and replace with the correct value in all files in src
RUN find . -type f -exec sed -i 's/$NODE_NAME_PREFIX/'$node_name_prefix'/g' {} ';'

# Build the project
RUN make clean
RUN make

CMD ["tmux", "new-session", "./idle.sh"]
