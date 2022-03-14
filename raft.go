package raft

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type StateRole int

const (
	Follower StateRole = iota
	Candidate
	Leader
)

func NewRaftNode(
	id int,
	peers map[int]RpcClient,
	electionTimeoutInMilli,
	heartbeatTimeoutInMilli int,
	stateMachine StateMachine,
	storage Storage,
	logger *logrus.Logger,
) (*RaftNode, error) {

	raftLog, err := newRaftLog(storage, stateMachine, logger)
	if err != nil {
		return nil, err
	}
	return &RaftNode{
		Id:                      id,
		peers:                   peers,
		currTerm:                0,
		votedFor:                0,
		raftLog:                 raftLog,
		quorum:                  (len(peers)+1)/2 + 1,
		stateRole:               Follower,
		electionTimeoutInMilli:  electionTimeoutInMilli,
		HeartbeatTimeoutInMilli: heartbeatTimeoutInMilli,
		heartbeatCh:             make(chan struct{}, 1),
		logger:                  logger,
	}, nil
}

type RaftNode struct {
	Id       int
	leaderId int
	peers    map[int]RpcClient
	// 服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增）
	currTerm uint64
	// 当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为 0
	votedFor int
	raftLog  *raftLog
	// 法定人数 (raft 默认是大多数节点)
	quorum                  int
	stateRole               StateRole
	electionTimeoutInMilli  int
	HeartbeatTimeoutInMilli int

	heartbeatCh chan struct{}

	// commitCh<logIdx, 通知客户端消息已被提交的 channel>
	commitCh map[uint64]chan<- struct{}

	shutdownCh chan struct{}

	logger *logrus.Logger
}

func (rn *RaftNode) Shutdown() {
	rn.shutdownCh <- struct{}{}
}

func (rn *RaftNode) HandleAppend(command []byte) (<-chan struct{}, error) {
	idx, err := rn.raftLog.Append(command, rn.getCurrTerm())
	if err != nil {
		rn.logger.WithError(err).Errorln("Failed to append command")
		return nil, err
	}
	// TODO lock
	commitCh := make(chan struct{}, 1)
	rn.commitCh[idx] = commitCh
	return commitCh, nil
}

// HandleAppendEntries 处理追加条目请求
func (rn *RaftNode) HandleAppendEntries(req *AppendEntriesReq) *AppendEntriesResp {
	currTerm := rn.getCurrTerm()
	resp := &AppendEntriesResp{
		Term:    currTerm,
		Success: false,
	}

	// 返回假 如果领导人的任期小于接收者的当前任期（5.1 节）
	if req.Term < currTerm {
		return resp
	}

	// 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，则令 currentTerm = T，并切换为跟随者状态（5.1 节）
	if req.Term > currTerm {
		rn.changeTerm(req.Term)
		rn.becomeFollower()
	}

	if rn.getStateRole() == Follower {
		rn.heartbeatCh <- struct{}{}
	}
	rn.setLeaderId(req.LeaderId)
	ok, err := rn.raftLog.AppendEntries(req.PrevLogIdx, req.PrevLogTerm, req.LeaderCommit, req.Entries)
	if err != nil {
		rn.logger.WithError(err).Errorln("Failed to handle append entries request")
		return resp
	}
	resp.Success = ok
	return resp
}

// HandleRequestVote 处理请求投票的请求
func (rn *RaftNode) HandleRequestVote(req *RequestVoteReq) *RequestVoteResp {
	currTerm := rn.getCurrTerm()
	resp := &RequestVoteResp{
		Term:        currTerm,
		VoteGranted: false,
	}

	// 如果term < currentTerm返回 false （5.2 节）
	if req.Term < currTerm {
		return resp
	}

	// 如果接收到的 RPC 请求或响应中，任期号T > currentTerm，则令 currentTerm = T，并切换为跟随者状态（5.1 节）
	if req.Term > currTerm {
		rn.changeTerm(req.Term)
		rn.becomeFollower()
	}

	// 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
	// TODO votedFor Lock
	if rn.votedFor == 0 || rn.votedFor == req.CandidateId {
		isUpToDate, err := rn.raftLog.IsUpToDate(req.LastLogIdx, req.LastLogTerm)
		if err != nil {
			rn.logger.WithError(err).Errorln("Failed to handle request vote request")
			return resp
		}
		resp.VoteGranted = isUpToDate
		if resp.VoteGranted {
			rn.votedFor = req.CandidateId
		}
	}
	return resp
}

func (rn *RaftNode) Run() {
	for {
		select {
		case <-rn.shutdownCh:
			return
		default:
		}
		switch rn.getStateRole() {
		case Follower:
			rn.runFollower()
		case Candidate:
			rn.runCandidate()
		case Leader:
			rn.runLeader()
		}
	}
}

func (rn *RaftNode) runFollower() {

	timeout := time.After(randomTimeout(rn.HeartbeatTimeoutInMilli))
	for rn.getStateRole() == Follower {
		select {
		case <-rn.heartbeatCh:
			// 接收到心跳，重置心跳计时器
			timeout = time.After(randomTimeout(rn.HeartbeatTimeoutInMilli))
		case <-timeout:
			// 接收心跳超时
			rn.logger.Infoln("heartbeat timeout")
			rn.becomeCandidate()
			return
		case <-rn.shutdownCh:
			return
		}
	}
}

func (rn *RaftNode) runCandidate() {
	// 赢得的选票初始化为1 相当于给自己投了一票
	grantedVotes := 1
	ctx, cancel := context.WithTimeout(context.Background(), randomTimeout(rn.electionTimeoutInMilli))
	defer cancel()

	// 自增当前的任期号（currentTerm）
	rn.incrCurrTerm()
	// TODO LOCK
	rn.votedFor = rn.Id

	// 发送请求投票的 RPC 给其他所有服务器
	lastEntryIdx, lastEntryTerm := rn.raftLog.LastEntry()
	req := &RequestVoteReq{
		Term:        rn.getCurrTerm(),
		CandidateId: rn.Id,
		LastLogIdx:  lastEntryIdx,
		LastLogTerm: lastEntryTerm,
	}
	respCh := rn.broadcastRequestVoteReq(ctx, req)

	for rn.getStateRole() == Candidate {
		select {
		case resp, ok := <-respCh:
			if !ok {
				return
			}
			rn.changeTerm(resp.Term)

			if resp.VoteGranted {
				grantedVotes++
			}
			if grantedVotes >= rn.quorum {
				// 选举成功
				rn.becomeLeader()
				return
			}
		case <-ctx.Done():
			// 选举超时
			return
		case <-rn.shutdownCh:
			return
		}
	}
}

func (rn *RaftNode) runLeader() {
	// leader state
	// 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1）
	nextIdx := make(map[int]uint64)
	lastLogIdx, _ := rn.raftLog.LastEntry()
	for peerId := range rn.peers {
		nextIdx[peerId] = lastLogIdx + 1
	}
	// 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
	matchIdx := make(map[int]uint64)
	timer := time.Tick(time.Duration(rn.HeartbeatTimeoutInMilli/2) * time.Millisecond)
	timeout := time.Duration(rn.HeartbeatTimeoutInMilli*3/2) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	respCh := rn.broadcastAppendEntriesReq(ctx, nextIdx)
	for rn.getStateRole() == Leader {
		select {
		case <-timer:
			cancel()
			ctx, cancel = context.WithTimeout(context.Background(), timeout)
			respCh = rn.broadcastAppendEntriesReq(ctx, nextIdx)
		case resp, ok := <-respCh:
			if !ok {
				continue
			}

			rn.changeTerm(resp.Term)
			if !resp.Success {
				// 如果因为日志不一致而失败，则 nextIndex 递减并重试
				if nextIdx[resp.peerId] > 0 {
					nextIdx[resp.peerId]--
				}
				continue
			}
			// 如果成功：更新相应跟随者的 nextIndex 和 matchIndex
			nextIdx[resp.peerId] = resp.nextIdx
			if resp.nextIdx > 0 {
				matchIdx[resp.peerId] = resp.nextIdx - 1
			}
			newIdx := findMajorityIdx(matchIdx)
			if ok, oldIdx := rn.raftLog.CommitTo(newIdx); ok {
				for i := oldIdx + 1; i <= newIdx; i++ {
					// TODO LOCK
					// 通知客户端新消息提交成功
					rn.commitCh[i] <- struct{}{}
				}
			}

		case <-rn.shutdownCh:
			return
		}
	}
}

func (rn *RaftNode) becomeFollower() {
	rn.logger.Infof("become follower.")
	rn.setStateRole(Follower)
}

func (rn *RaftNode) becomeCandidate() {
	rn.logger.Infoln("become candidate")
	rn.setStateRole(Candidate)
}

func (rn *RaftNode) becomeLeader() {
	rn.logger.Infoln("become leader")
	// TODO LOCK
	rn.commitCh = make(map[uint64]chan<- struct{})
	rn.setLeaderId(rn.Id)
	rn.setStateRole(Leader)
}

func (rn *RaftNode) IsLeader() bool {
	return rn.getStateRole() == Leader
}

func (rn *RaftNode) LeaderNodeName() (string, bool) {
	if rn.getLeaderId() == 0 {
		return "", false
	}
	return rn.peers[rn.getLeaderId()].NodeName(), true
}

func (rn *RaftNode) getStateRole() StateRole {
	// TODO atomic
	return rn.stateRole
}

func (rn *RaftNode) setStateRole(newRole StateRole) {
	// TODO atomic
	rn.stateRole = newRole
}

func (rn *RaftNode) getCurrTerm() uint64 {
	// TODO atomic
	return rn.currTerm
}

func (rn *RaftNode) incrCurrTerm() {
	// TODO atomic
	rn.currTerm++
}

func (rn *RaftNode) changeTerm(newTerm uint64) {
	// todo LOCK
	if newTerm > rn.currTerm {
		rn.logger.Infof("the term has changed. new term: %d.", newTerm)
		rn.currTerm = newTerm
		rn.votedFor = 0
	}
}

func (rn *RaftNode) getLeaderId() int {
	// TODO atomic
	return rn.leaderId
}

func (rn *RaftNode) setLeaderId(leaderId int) {
	// TODO atomic
	if rn.leaderId != leaderId {
		rn.logger.Infof("new leader id: %d.", leaderId)
		rn.leaderId = leaderId
	}
}

func (rn *RaftNode) broadcastRequestVoteReq(ctx context.Context, req *RequestVoteReq) <-chan *RequestVoteResp {
	ch := make(chan *RequestVoteResp, len(rn.peers))
	var wg sync.WaitGroup
	wg.Add(len(rn.peers))
	defer func() {
		go func() {
			wg.Wait()
			close(ch)
		}()
	}()
	for peerId, peer := range rn.peers {
		go func(peerId int, peer RpcClient) {
			defer wg.Done()
			resp, err := peer.RequestVote(ctx, req)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					rn.logger.WithError(err).WithFields(logrus.Fields{
						"node_id": peerId,
						"node":    peer.NodeName(),
					}).Errorln("Failed to send request vote request")
				}
				resp = &RequestVoteResp{
					Term:        req.Term,
					VoteGranted: false,
				}
			}
			ch <- resp
		}(peerId, peer)
	}
	return ch
}

type AppendEntriesRes struct {
	*AppendEntriesResp
	peerId  int
	nextIdx uint64
}

func (rn *RaftNode) broadcastAppendEntriesReq(ctx context.Context, nextIdx map[int]uint64) <-chan AppendEntriesRes {
	currTerm := rn.getCurrTerm()
	leaderId := rn.getLeaderId()
	leaderCommit := rn.raftLog.commitIdx

	ch := make(chan AppendEntriesRes, len(rn.peers))
	var wg sync.WaitGroup
	wg.Add(len(rn.peers))
	defer func() {
		go func() {
			wg.Wait()
			close(ch)
		}()
	}()

	for peerId, peer := range rn.peers {
		go func(peerId int, peer RpcClient) {
			defer wg.Done()
			prevLogIdx, prevLogTerm, entries, err := rn.raftLog.PackEntries(nextIdx[peerId], 100)
			if err != nil {
				rn.logger.WithError(err).WithFields(logrus.Fields{
					"peerId":  peerId,
					"nextIdx": nextIdx[peerId],
				}).Errorln("failed to pack entries")
				return
			}
			var resp *AppendEntriesResp
			resp, err = peer.AppendEntries(ctx, &AppendEntriesReq{
				Term:         currTerm,
				LeaderId:     leaderId,
				PrevLogIdx:   prevLogIdx,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: leaderCommit,
			})
			if err != nil {
				if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
					rn.logger.WithError(err).WithFields(logrus.Fields{
						"node_id": peerId,
						"node":    peer.NodeName(),
					}).Errorln("Failed to send append entries request")
				}
				return
			}
			ch <- AppendEntriesRes{
				AppendEntriesResp: resp,
				peerId:            peerId,
				nextIdx:           prevLogIdx + uint64(len(entries)) + 1,
			}
		}(peerId, peer)
	}
	return ch
}

type AppendEntriesReq struct {
	// 领导人的任期
	Term uint64 `json:"term"`
	// 领导人 ID 因此跟随者可以对客户端进行重定向
	LeaderId int `json:"leader_id"`
	// 紧邻新日志条目之前的那个日志条目的索引
	PrevLogIdx uint64 `json:"prev_log_idx"`
	// 紧邻新日志条目之前的那个日志条目的任期
	PrevLogTerm uint64 `json:"prev_log_term"`
	// 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个）
	Entries []*Entry `json:"entries"`
	// 领导人的已知已提交的最高的日志条目的索引
	LeaderCommit uint64 `json:"leader_commit"`
}

type AppendEntriesResp struct {
	// 当前任期，对于领导人而言 它会更新自己的任期
	Term uint64 `json:"term"`
	// 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true
	Success bool `json:"success"`
}

type RequestVoteReq struct {
	// 候选人的任期号
	Term uint64 `json:"term"`
	// 请求选票的候选人的 ID
	CandidateId int `json:"candidate_id"`
	// 候选人的最后日志条目的索引值
	LastLogIdx uint64 `json:"last_log_idx"`
	// 候选人最后日志条目的任期号
	LastLogTerm uint64 `json:"last_log_term"`
}

type RequestVoteResp struct {
	// 当前任期号，以便于候选人去更新自己的任期号
	Term uint64 `json:"term"`
	// 候选人赢得了此张选票时为真
	VoteGranted bool `json:"vote_granted"`
}

func randomTimeout(timeoutInMilli int) time.Duration {
	return time.Duration(timeoutInMilli+rand.Intn(timeoutInMilli)) * time.Millisecond
}

type Uint64Slice []uint64

func (s Uint64Slice) Len() int           { return len(s) }
func (s Uint64Slice) Less(i, j int) bool { return s[i] < s[j] }
func (s Uint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func findMajorityIdx(nextIdx map[int]uint64) uint64 {
	if len(nextIdx) == 0 {
		return 0
	}
	s := make(Uint64Slice, len(nextIdx))
	i := 0
	for _, idx := range nextIdx {
		s[i] = idx
		i++
	}
	if len(s) == 1 {
		return s[0]
	}
	sort.Sort(s)
	return s[len(nextIdx)/2]
}
