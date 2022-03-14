package raft

import "github.com/vmihailenco/msgpack/v5"

type StateMachine interface {
	Apply(command []byte) error
}

func NewMemoryStateMachine() *MemoryStateMachine {
	return &MemoryStateMachine{
		data: make(map[string]string),
	}
}

// MsmOp memory State machine op
type MsmOp int

const (
	MsmSet MsmOp = iota
	MsmRm
)

// MsmCommand memory state machine command
type MsmCommand struct {
	Op    MsmOp
	Key   string
	Value string
}

func (c MsmCommand) Marshal() ([]byte, error) {
	return msgpack.Marshal(&c)
}

type MemoryStateMachine struct {
	data map[string]string
}

func (m *MemoryStateMachine) Apply(command []byte) error {
	var cmd MsmCommand
	if err := msgpack.Unmarshal(command, &cmd); err != nil {
		return err
	}

	switch cmd.Op {
	case MsmSet:
		m.data[cmd.Key] = cmd.Value
	case MsmRm:
		delete(m.data, cmd.Key)
	}

	return nil
}

func (m *MemoryStateMachine) Get(key string) (v string, ok bool) {
	v, ok = m.data[key]
	return
}
