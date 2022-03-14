package main

import (
	"0x5459/raft-go"
	"context"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/imroc/req/v3"
	"github.com/mattn/go-colorable"
	"github.com/sirupsen/logrus"
	"log"
	"net/http"
	"net/url"
	"sort"
	"time"
)

const (
	PathAppend        = "/append"
	PathGetKey        = "/get_key"
	PathAppendEntries = "/raft/append_entries"
	PathRequestVote   = "/raft/request_vote"
)

type RaftError struct {
	ErrNo int    `json:"err_no"`
	Err   string `json:"error"`
	Data  string `json:"leader_url"`
}

func raftError(err error) RaftError {
	return RaftError{Err: err.Error()}
}

func notLeader(leaderUrl string) RaftError {
	return RaftError{
		ErrNo: 1,
		Err:   "not leader",
		Data:  leaderUrl,
	}
}

type Entry struct {
	Term uint64 `json:"term"`
	Idx  uint64 `json:"idx"`
	Data string `json:"data"` // base64
}

type AppendEntriesReq struct {
	Term         uint64   `json:"term"`
	LeaderId     int      `json:"leader_id"`
	PrevLogIdx   uint64   `json:"prev_log_idx"`
	PrevLogTerm  uint64   `json:"prev_log_term"`
	Entries      []*Entry `json:"entries"`
	LeaderCommit uint64   `json:"leader_commit"`
}

func toRaftAppendEntriesReq(httpReq *AppendEntriesReq) (*raft.AppendEntriesReq, error) {
	entries := make([]*raft.Entry, len(httpReq.Entries))
	for i, e := range httpReq.Entries {
		data := make([]byte, base64.URLEncoding.DecodedLen(len(e.Data)))
		if _, err := base64.URLEncoding.Decode(data, []byte(e.Data)); err != nil {
			return nil, fmt.Errorf("invalid base64 data, idx: %d", e.Idx)
		}
		entries[i] = &raft.Entry{
			Term: e.Term,
			Idx:  e.Idx,
			Data: data,
		}
	}
	return &raft.AppendEntriesReq{
		Term:         httpReq.Term,
		LeaderId:     httpReq.LeaderId,
		PrevLogIdx:   httpReq.PrevLogIdx,
		PrevLogTerm:  httpReq.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: httpReq.LeaderCommit,
	}, nil
}

func toHttpAppendEntriesReq(raftReq *raft.AppendEntriesReq) *AppendEntriesReq {
	entries := make([]*Entry, len(raftReq.Entries))
	for i, e := range raftReq.Entries {
		entries[i] = &Entry{
			Term: e.Term,
			Idx:  e.Idx,
			Data: base64.URLEncoding.EncodeToString(e.Data),
		}
	}
	return &AppendEntriesReq{
		Term:         raftReq.Term,
		LeaderId:     raftReq.LeaderId,
		PrevLogIdx:   raftReq.PrevLogIdx,
		PrevLogTerm:  raftReq.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: raftReq.LeaderCommit,
	}
}

func RegisterServerHandler(router *gin.Engine, raftNode *raft.RaftNode, stateMachine *raft.MemoryStateMachine) {
	checkLeader := func(c *gin.Context) bool {
		if !raftNode.IsLeader() {
			leader, ok := raftNode.LeaderNodeName()
			if !ok {
				c.JSON(http.StatusInternalServerError, raftError(errors.New("no leader")))
				return false
			}
			c.JSON(http.StatusBadRequest, notLeader(leader))
			return false
		}
		return true
	}
	router.GET(PathGetKey, func(c *gin.Context) {
		if !checkLeader(c) {
			return
		}
		key := c.Query("key")
		if key == "" {
			c.JSON(http.StatusInternalServerError, raftError(errors.New("key cannot be empty")))
			return
		}
		v, ok := stateMachine.Get(key)
		c.JSON(http.StatusOK, gin.H{
			"exist": ok,
			"value": v,
		})
	})
	router.POST(PathAppend, func(c *gin.Context) {
		if !checkLeader(c) {
			return
		}
		type AppendReq struct {
			Command string `json:"command"`
		}
		var appendReq AppendReq
		if err := c.ShouldBind(&appendReq); err != nil {
			c.JSON(http.StatusBadRequest, raftError(err))
			return
		}
		command := make([]byte, base64.URLEncoding.DecodedLen(len(appendReq.Command)))
		if _, err := base64.URLEncoding.Decode(command, []byte(appendReq.Command)); err != nil {
			c.JSON(http.StatusBadRequest, raftError(errors.New("invalid base64 command")))
			return
		}
		okCh, err := raftNode.HandleAppend(command)
		if err != nil {
			c.JSON(http.StatusInternalServerError, raftError(err))
			return
		}
		select {
		case <-okCh:
			c.Status(http.StatusNoContent)
		case <-time.After(time.Duration(raftNode.HeartbeatTimeoutInMilli) * time.Millisecond):
			c.JSON(http.StatusInternalServerError, raftError(errors.New("timeout")))
		}
	})

	router.POST(PathAppendEntries, func(c *gin.Context) {
		var appendEntriesReq AppendEntriesReq
		if err := c.ShouldBind(&appendEntriesReq); err != nil {
			c.JSON(http.StatusBadRequest, raftError(err))
			return
		}
		raftReq, err := toRaftAppendEntriesReq(&appendEntriesReq)
		if err != nil {
			c.JSON(http.StatusBadRequest, raftError(err))
			return
		}
		c.JSON(http.StatusOK, raftNode.HandleAppendEntries(raftReq))
	})

	router.POST(PathRequestVote, func(c *gin.Context) {
		var requestVoteReq raft.RequestVoteReq
		if err := c.ShouldBind(&requestVoteReq); err != nil {
			c.JSON(http.StatusBadRequest, raftError(err))
			return
		}
		c.JSON(http.StatusOK, raftNode.HandleRequestVote(&requestVoteReq))
	})
}

func NewHttpClient(peerUrl string, c *req.Client) (raft.RpcClient, error) {
	u, err := url.Parse(peerUrl)
	if err != nil {
		return nil, err
	}
	u.RawQuery = ""
	u.Fragment = ""
	return &httpRpcClient{
		client:  c,
		peerUrl: u.String(),
	}, nil
}

type httpRpcClient struct {
	client  *req.Client
	peerUrl string
}

func (c *httpRpcClient) NodeName() string {
	return c.peerUrl
}

func (c *httpRpcClient) RequestVote(ctx context.Context, requestVoteReq *raft.RequestVoteReq) (*raft.RequestVoteResp, error) {
	var body raft.RequestVoteResp
	var raftErr RaftError
	resp, err := c.client.R().
		SetBodyJsonMarshal(requestVoteReq).
		SetResult(&body).
		SetError(&raftErr).
		SetContext(ctx).
		Post(c.peerUrl + PathRequestVote)
	if err != nil {
		return nil, err
	}
	if !resp.IsSuccess() {
		return nil, errors.New(raftErr.Err)
	}
	return &body, nil
}

func (c *httpRpcClient) AppendEntries(ctx context.Context, raftReq *raft.AppendEntriesReq) (*raft.AppendEntriesResp, error) {

	var body raft.AppendEntriesResp
	var raftErr RaftError
	resp, err := c.client.R().
		SetBodyJsonMarshal(toHttpAppendEntriesReq(raftReq)).
		SetResult(&body).
		SetError(&raftErr).
		SetContext(ctx).
		Post(c.peerUrl + PathAppendEntries)

	if err != nil {
		return nil, err
	}
	if !resp.IsSuccess() {
		return nil, errors.New(raftErr.Err)
	}
	return &body, nil
}

type peersFlags []string

func (i *peersFlags) String() string {
	return ""
}

func (i *peersFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func main() {
	electionTimeoutInMilli := flag.Int("electionTimeoutInMilli", 100, "election timeout (Millisecond)")
	heartbeatTimeoutInMilli := flag.Int("heartbeatTimeoutInMilli", 100, "heartbeat timeout (Millisecond)")

	localUrl := flag.String("local", "", "local url")
	var peerUrls peersFlags
	flag.Var(&peerUrls, "peers", "peer urls")
	flag.Parse()

	if *localUrl == "" {
		log.Fatal("local url cannot be empty")
	}

	_ = peerUrls.Set(*localUrl)
	sort.Strings(peerUrls)

	reqClient := req.C()
	peers := make(map[int]raft.RpcClient)
	id := 0
	for i, peerUrl := range peerUrls {
		if peerUrl == *localUrl {
			id = i + 1
			continue
		}
		if rpcClient, err := NewHttpClient(peerUrl, reqClient); err != nil {
			panic(fmt.Errorf("invalid peer url: (%s); err:(%s)", peerUrl, err.Error()))
		} else {
			peers[i+1] = rpcClient
		}
	}
	stateMachine := raft.NewMemoryStateMachine()
	raftNode, err := raft.NewRaftNode(id, peers, *electionTimeoutInMilli, *heartbeatTimeoutInMilli, stateMachine, raft.NewMemoryStorage(), setupLogger())
	if err != nil {
		panic(fmt.Errorf("filed to create raft node. err: %s", err.Error()))
	}
	go raftNode.Run()
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(gin.Recovery())
	RegisterServerHandler(r, raftNode, stateMachine)

	addr, err := url.Parse(*localUrl)
	if err != nil {
		panic(err)
	}
	log.Fatal(r.Run(addr.Host))
}

func setupLogger() *logrus.Logger {
	logger := logrus.StandardLogger()
	logger.SetLevel(logrus.DebugLevel)
	logrus.SetOutput(colorable.NewColorableStdout())
	logger.SetFormatter(&logrus.TextFormatter{
		ForceColors:   true,
		FullTimestamp: true,
	})
	return logger
}
