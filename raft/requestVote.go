package raft

import (
    "context"
    "sync/atomic"
)

// raft server 响应 RequestVote RPC
func (rf *Raft) RequestVote(ctx context.Context, args *RequestVoteArgs) (*RequestVoteReply, error) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    defer rf.saveState()

    DPrintf("[RecRequestVote]%v[%v] to %v[%v]", args.CandidateId, args.Term, rf.me, rf.currTerm)
    reply := new(RequestVoteReply)

    // candidate's term < currentTerm, vote denied
    if args.Term < rf.currTerm {
        reply.Term = rf.currTerm
        reply.Granted = false
        return reply, nil
    }
    // args term > currentTerm, rf reverts to its follower and reset votedFor
    if args.Term > rf.currTerm {
        rf.currTerm = args.Term
        rf.changeStateTo(Follower)
    }

    // when args term >= currentTerm, need additional judgement
    // return 'current term', in face is args.Term too
    reply.Term = rf.currTerm
    // 1. compare voteFor id
    if rf.votedFor != NullVotedFor && rf.votedFor != args.CandidateId {
        reply.Granted = false
        return reply, nil
    }
    // 2. compare which log is more updated
    rfLastIndex := int64(len(rf.logs)) - 1
    rfLastTerm := rf.logs[rfLastIndex].Term
    if rfLastTerm < args.LastLogTerm || (rfLastTerm == args.LastLogTerm && rfLastIndex <= rf.getCurrIndex(args.LastLogIndex)) {
        reply.Granted = true
        rf.votedFor = args.CandidateId
        // grant vote, need reset election timer
        rf.electionTimer.Reset(getRandElectionTimeout())
    } else {
        reply.Granted = false
    }
    return reply, nil
}

// raft client 发起 RequestVote RPC
func (rf *Raft) sendRequestVote(server string, ctx context.Context, args *RequestVoteArgs, reply *RequestVoteReply) error {
    client, err := rf.getPeerClient(server)
    if err != nil {
        return err
    }

    res, err := client.RequestVote(ctx, args)
    if err != nil {
        return err
    }

    res.CopyToRaft(reply)
    return nil
}

// doRequestVote Candidate 开始选举
// 由调用者加锁
func (rf *Raft) doRequestVote() {
    rf.currTerm++
    rf.votedFor = rf.me
    rf.saveState()

    term := rf.currTerm
    candidateId := rf.me
    lastLogIndex := rf.getRealIndex(int64(len(rf.logs)) - 1)
    lastLogTerm := rf.logs[rf.getCurrIndex(lastLogIndex)].Term

    // 使用原子操作来统计票数
    var grantVoteCount int32

    // send RequestVoteRPC in parallel
    for i := range rf.peers {
        if i == rf.me {
            continue
        }

        // 使用 goroutine 并行发送投票请求
        go func(peerAddr string) {
            args := &RequestVoteArgs{
                Term:          term,
                CandidateId:   candidateId,
                LastLogIndex:  lastLogIndex,
                LastLogTerm:   lastLogTerm,
            }
            reply := new(RequestVoteReply)
            DPrintf("[SendAppendEntries]%v[%v] to %v", candidateId, term, peerAddr)

            // 发送投票
            err := rf.sendRequestVote(peerAddr, nil, args, reply)
            if err != nil {

            }

            rf.mu.Lock()
            defer rf.mu.Unlock()
            if rf.currTerm < reply.Term {
                rf.currTerm = reply.Term
                rf.changeStateTo(Follower)
                rf.electionTimer.Reset(getRandElectionTimeout())
                rf.saveState()
                return
            }
            // 防止过期的 reply
            if rf.currTerm != args.Term {
                return
            }
            if reply.Granted && rf.state == Candidate {
                atomic.AddInt32(&grantVoteCount, 1)
                if int(atomic.LoadInt32(&grantVoteCount)) >= len(rf.peers) / 2 {
                    rf.changeStateTo(Leader)
                }
            }
        }(i)
    }
}
