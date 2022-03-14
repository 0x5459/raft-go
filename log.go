package raft

import (
	"github.com/sirupsen/logrus"
	"math"
)

type Entry struct {
	Term uint64 `json:"term"`
	Idx  uint64 `json:"idx"`
	Data []byte `json:"data"`
}

func newRaftLog(storage Storage, stateMachine StateMachine, logger *logrus.Logger) (*raftLog, error) {
	lastEntry, err := storage.Last()
	if err != nil {
		return nil, err
	}
	return &raftLog{
		storage:       storage,
		commitIdx:     0,
		lastApplied:   0,
		lastEntryIdx:  lastEntry.Idx,
		lastEntryTerm: lastEntry.Term,
		stateMachine:  stateMachine,
		logger:        logger,
	}, nil
}

type raftLog struct {
	storage      Storage
	stateMachine StateMachine
	// 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	commitIdx uint64
	// 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied uint64

	lastEntryIdx  uint64
	lastEntryTerm uint64
	logger        *logrus.Logger
}

func (rl *raftLog) Append(command []byte, term uint64) (uint64, error) {
	// TODO LOCK
	idx := rl.lastEntryIdx + 1
	err := rl.storage.TruncateAndAppend([]*Entry{{
		Term: term,
		Idx:  idx,
		Data: command,
	}})
	if err == nil {
		rl.logger.Debugf("Append log index: %d", idx)
		rl.lastEntryIdx = idx
		rl.lastEntryTerm = term
	}
	return idx, err
}

func (rl *raftLog) AppendEntries(
	prevLogIdx,
	prevLogTerm,
	leaderCommit uint64,
	entries []*Entry,
) (ok bool, err error) {
	// 返回假 如果接收者日志中没有包含这样一个条目
	// 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上（5.3 节）
	var term uint64
	if term, err = rl.storage.Term(prevLogIdx); err != nil || term != prevLogTerm {
		return
	}
	if len(entries) != 0 {

		// 如果一个已经存在的条目和新条目发生了冲突（因为索引相同，任期不同），
		// 那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节）
		var conflictIdx uint64
		if conflictIdx, err = rl.findConflict(entries); err != nil {
			return
		}
		entries = entries[conflictIdx-entries[0].Idx:]
		if len(entries) != 0 {
			rl.logger.DebugFn(func() []interface{} {
				idxs := make([]uint64, len(entries))
				for i, entry := range entries {
					idxs[i] = entry.Idx
				}
				return []interface{}{"Append entries index: ", idxs}
			})
			last := entries[len(entries)-1]
			rl.lastEntryIdx, rl.lastEntryTerm = last.Idx, last.Term
			if err = rl.storage.TruncateAndAppend(entries); err != nil {
				return
			}
		}
	}
	// 如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引（leaderCommit > commitIndex），
	// 则把接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为
	// 领导人的已知已经提交的最高的日志条目的索引 leaderCommit 或者是 上一个新条目的索引 取两者的最小值
	if leaderCommit > rl.commitIdx {
		rl.CommitTo(uint64(math.Min(float64(prevLogIdx+uint64(len(entries))), float64(leaderCommit))))
	}
	ok = true
	return
}

// IsUpToDate 返回给定的日志索引和日志任期是否至少比当前 storage 中存储的日志新
func (rl *raftLog) IsUpToDate(logIdx, logTerm uint64) (bool, error) {
	lastEntryIdx, lastEntryTerm := rl.LastEntry()
	ok := logTerm > lastEntryTerm || (logTerm == lastEntryTerm && logIdx >= lastEntryIdx)
	return ok, nil
}

func (rl *raftLog) findConflict(entries []*Entry) (uint64, error) {
	for _, entry := range entries {
		if term, err := rl.storage.Term(entry.Idx); err != nil || term != entry.Term {
			return entry.Idx, err
		}

	}
	return entries[0].Idx, nil
}

func (rl *raftLog) LastEntry() (uint64, uint64) {
	return rl.lastEntryIdx, rl.lastEntryTerm
}

func (rl *raftLog) PackEntries(nextId uint64, maxLen int) (prevLogIdx uint64, prevLogTerm uint64, entries []*Entry, err error) {
	start := nextId
	if start > 0 {
		start--
	}
	entries, err = rl.storage.Entries(start, nextId+uint64(maxLen))
	if err != nil {
		return
	}
	if len(entries) != 0 {
		prevLogIdx, prevLogTerm = entries[0].Idx, entries[0].Term
		entries = entries[1:]
	} else {
		prevLogIdx, prevLogTerm = rl.lastEntryIdx, rl.lastEntryTerm
	}
	return
}

func (rl *raftLog) CommitTo(idx uint64) (ok bool, oldIdx uint64) {
	// todo lock
	if idx > rl.commitIdx {
		oldIdx = rl.commitIdx
		rl.commitIdx = idx
		ok = true
		// 如果commitIndex > lastApplied，则 lastApplied 递增，并将log[lastApplied]应用到状态机中（5.3 节）
		if rl.lastApplied < idx {
			entries, err := rl.storage.Entries(rl.lastApplied+1, idx+1)
			if err != nil {
				rl.logger.WithError(err).Errorln("failed to load entries")
			} else {
				for _, entry := range entries {
					if err := rl.stateMachine.Apply(entry.Data); err != nil {
						rl.logger.WithError(err).Errorln("failed to apply command to state machine")
					}
				}
			}
			rl.lastApplied = idx
		}
	} else {
		ok = false
	}
	return
}
