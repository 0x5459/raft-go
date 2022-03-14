package raft

import (
	"math"
	"sync"
)

type Storage interface {
	// Term 返回指定索引日志的任期
	Term(idx uint64) (uint64, error)

	// Last 返回最新的日志项
	Last() (*Entry, error)

	// Entries 返回日志项切片
	Entries(start, end uint64) ([]*Entry, error)

	TruncateAndAppend(entries []*Entry) error
}

func NewMemoryStorage() Storage {
	return &memoryStorage{
		// Storage 里默认存一个索引为 0 任期也为 0 的日志条目便于简化操作
		entries: []*Entry{
			{
				Term: 0,
				Idx:  0,
				Data: []byte{},
			},
		},
	}
}

type memoryStorage struct {
	rwLock  sync.RWMutex
	entries []*Entry
}

func (ms *memoryStorage) Term(idx uint64) (uint64, error) {
	ms.rwLock.RLock()
	defer ms.rwLock.RUnlock()
	if idx >= uint64(len(ms.entries)) {
		return 0, nil
	}
	return ms.entries[idx].Term, nil
}

func (ms *memoryStorage) Last() (*Entry, error) {
	ms.rwLock.RLock()
	defer ms.rwLock.RUnlock()
	return ms.entries[len(ms.entries)-1], nil
}

func (ms *memoryStorage) Entries(start, end uint64) ([]*Entry, error) {
	ms.rwLock.RLock()
	defer ms.rwLock.RUnlock()
	var slice []*Entry
	if end > uint64(len(ms.entries)) {
		slice = ms.entries[start:]
	} else {
		slice = ms.entries[start:end]
	}
	ret := make([]*Entry, len(slice))
	copy(ret, slice)
	return ret, nil
}

func (ms *memoryStorage) TruncateAndAppend(entries []*Entry) error {
	toAppendSize := uint64(len(entries))
	if toAppendSize == 0 {
		return nil
	}

	after := entries[0].Idx

	ms.rwLock.Lock()
	defer ms.rwLock.Unlock()

	size := uint64(len(ms.entries))
	if after == size {
		ms.entries = append(ms.entries, entries...)
	} else {
		n := uint64(math.Min(float64(toAppendSize), float64(size-after)))
		var i uint64
		for i = 0; i < n; i++ {
			ms.entries[after+i] = entries[i]
		}
		if toAppendSize > size-after {
			ms.entries = append(ms.entries, entries[size-after:]...)
		} else if toAppendSize < size-after {
			ms.entries = ms.entries[after+toAppendSize:]
		}
	}
	return nil
}
