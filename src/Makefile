
SRCS=$(wildcard *.go)

.PHONY : all
all : mp3

mp3 : $(SRCS) Makefile
	go build -o mp3 $(SRCS)

.PHONY : clean
clean:
	rm -rf mp3 mp2 mp1 go.mod
