package raft

import "context"

type RpcClient interface {
	NodeName() string
	AppendEntries(ctx context.Context, req *AppendEntriesReq) (*AppendEntriesResp, error)
	RequestVote(ctx context.Context, req *RequestVoteReq) (*RequestVoteResp, error)
}
