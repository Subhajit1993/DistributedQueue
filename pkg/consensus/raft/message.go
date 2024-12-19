package raft

import "time"

type Message struct {
	Id        string
	Data      []byte
	Timestamp time.Time
	Attempts  int
}
