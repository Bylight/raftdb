package raft

import (
    "context"
    "log"
    "sync/atomic"
    "time"
)

// raft dbserver 响应 AppendEntries RPC
func (rf *Raft) AppendEntries(context context.Context, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    DPrintf("[RecAppendEntries] %v[%v] to %v[%v]", args.LeaderId, args.Term, rf.me, rf.currTerm)
    reply := new(AppendEntriesReply)
    reply.Term = rf.currTerm

    // impl #1
    if args.Term < rf.currTerm {
        reply.Success = false
        return reply, nil
    }

    // 来自合法 Leader 的心跳
    rf.currTerm = args.Term
    if rf.state != Follower {
        rf.changeStateTo(Follower)
    }
    rf.electionTimer.Reset(getRandElectionTimeout())
    // RPC 可能是过时的
    if size := rf.lastIncludedIndex - args.PrevLogIndex; size > 0 {
        args.Term = -1
        rf.saveState()
        return reply, nil
    }

    // # 2
    // 这里对心跳响应进行优化，以快速达到一致
    currPrevLogIndex := rf.getCurrIndex(args.PrevLogIndex)
    // doesn't have PrevLogIndex
    if rf.getRealLogLen() < args.PrevLogIndex + 1 {
        reply.ConflictIndex = rf.getRealLogLen()
        reply.ConflictTerm = -1
        reply.Success = false
        rf.saveState()
        return reply, nil
    // have PrevLogIndex, but its term doesn't match
    } else if rf.logs[currPrevLogIndex].Term != args.PrevLogTerm {
        reply.ConflictTerm = rf.logs[currPrevLogIndex].Term
        for i, v := range rf.logs {
            if v.Term == reply.ConflictTerm {
                reply.ConflictIndex = rf.getRealIndex(int64(i))
                break
            }
        }
        reply.Success = true
        rf.saveState()
        return reply, nil
    }
    reply.Success = true

    // #3 #4 (心跳情况下不进行)
    if len(args.Entries) != 0 {
        // impl #3: compare the existing entry and coming entries
        currIndex := currPrevLogIndex // entries 下一步应该复制的位置
        for i := 0; i < len(args.Entries); i++ {
            currIndex++
            if int64(len(rf.logs)) <= currIndex {
                break
            }
            // 出现 entries 不一致，删除这之后的所有本地 entries
            if rf.logs[currIndex].Term != args.Entries[i].Term {
                rf.logs = rf.logs[:currIndex]
                break
            }
        }
        // impl #4: append any new entries not already in the log
        if int64(len(rf.logs)) <= currIndex {
            rf.logs = append(rf.logs, args.Entries[currIndex-currPrevLogIndex-1:]...)
        }
        DPrintf("[LogAppendedInRaftClient] dbclient %v", rf.me)
    }
    rf.saveState()

    // impl #5: set rf's commitIndex
    // notice the last "new" Entry
    if rf.commitIndex < args.LeaderCommitIndex {
        if args.LeaderCommitIndex < args.PrevLogIndex + int64(len(args.Entries)) {
            // 2B DPrintf("[CommitIndexUpdated]: peer[%v] update commitIndex [%v] to [%v]", rf.me, rf.commitIndex, args.LeaderCommit)
            rf.commitIndex = args.LeaderCommitIndex
        } else {
            // 2B DPrintf("[CommitIndexUpdated]: peer[%v] update commitIndex [%v] to [%v]", rf.me, rf.commitIndex, args.PrevLogIndex + len(args.Entries))
            rf.commitIndex = args.PrevLogIndex + int64(len(args.Entries))
        }
        rf.doApplyEntry()
    }
    return reply, nil
}

// raft dbclient 发起 AppendEntries RPC
func (rf *Raft) sendAppendEntries(server string, ctx context.Context, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
    client, err := rf.getPeerClient(server)
    if err != nil {
        return err
    }

    res, err := client.AppendEntries(ctx, args)
    if err != nil {
        return err
    }

    res.CopyToRaft(reply)
    return nil
}

// doAppendEntries Leader 发送 AppendEntries RPC 的具体行为
// 由调用者加锁
func (rf *Raft) doAppendEntries() {
    for i := range rf.peers {
        if i == rf.me {
            rf.nextIndex[i] = rf.getRealLogLen()
            rf.matchIndex[i] = rf.getRealLogLen() - 1
            continue
        }
        // 具体处理逻辑
        go rf.doAppendEntriesTo(i)
    }
}

// doAppendEntriesTo 对指定节点发送 AppendEntries RPC
func (rf *Raft) doAppendEntriesTo(peerAddr string) {
    rf.mu.Lock()
    if rf.state != Leader {
        rf.mu.Unlock()
        return
    }
    // 可能需要发送 snapshot
    if rf.nextIndex[peerAddr] <= rf.lastIncludedIndex {
        rf.doInstallSnapshotTo(peerAddr)
        return
    }
    args := new(AppendEntriesArgs)
    args.Term = rf.currTerm
    args.LeaderId = rf.me
    args.LeaderCommitIndex = rf.commitIndex
    // 每个节点有不同的数据
    args.PrevLogIndex = rf.nextIndex[peerAddr] - 1
    args.PrevLogTerm = rf.logs[rf.getCurrIndex(args.PrevLogIndex)].Term
    reply := new(AppendEntriesReply)
    // 由 nextIndex 和 matchIndex 判断是发送 Heartbeat 还是具体的 AppendEntries
    // 处理 AppendEntriesArgs 中的 Entries
    if args.PrevLogIndex != rf.matchIndex[peerAddr] || rf.nextIndex[peerAddr] >= rf.getRealLogLen() {
        args.Entries = []*LogEntry{}
    } else {
        args.Entries = rf.logs[rf.getCurrIndex(rf.nextIndex[peerAddr]):]
    }
    if len(args.Entries) > 0 {
        DPrintf("[SendAppendEntries]%v[%v] to %v, args.PrevLogIndex %v, len(args.Entries) %v", rf.me, rf.currTerm, peerAddr, args.PrevLogIndex, len(args.Entries))
    } else {
        DPrintf("[SendHeartbeat]%v[%v] to %v", rf.me, rf.currTerm, peerAddr)
    }
    rf.mu.Unlock()

    // 进行 RPC 调用
    err := rf.sendAppendEntries(peerAddr, context.Background(), args, reply)
    if err != nil {
        log.Println(err)
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()
    // fail because of out-dated term
    // 收到更大的 Term，Leader 退位
    if reply.Term > rf.currTerm {
        rf.currTerm = reply.Term
        rf.changeStateTo(Follower)
        // 重置选举 timer
        rf.electionTimer.Reset(getRandElectionTimeout())
        rf.saveState()
        return
    }
    // 处理回调结果
    // rf Term 与 args Term 不一致，说明是过期的 reply
    if rf.currTerm != args.Term || rf.state != Leader {
        return
    }
    // 维护 log consistency
    rf.maintainLogConsistency(peerAddr, args, reply)
}

// maintainLogConsistency 维护 Leader 中的 commitIndex, nextIndex 和 matchIndex
// 由调用者加锁
func (rf *Raft) maintainLogConsistency(peerIndex string, args *AppendEntriesArgs, reply *AppendEntriesReply)  {
    // update nextIndex and matchIndex
    if reply.Success {
        // update nextIndex and matchIndex
        // 注意，这里更新 matchIndex 时要用 args 中的数据（Leader 的 log 可能在 RPC 调用期间发生了变化）
        rf.matchIndex[peerIndex] = args.PrevLogIndex + int64(len(args.Entries))
        rf.nextIndex[peerIndex] = rf.matchIndex[peerIndex] + 1

        // 判断是否能够更新 commitIndex
        rf.updateCommitIndex()
    } else {
        rf.nextIndex[peerIndex] = reply.ConflictIndex
        if reply.ConflictTerm != -1 {
            for curr := len(rf.logs); curr > 0; curr-- {
                if rf.logs[curr- 1].Term == reply.ConflictTerm {
                    rf.nextIndex[peerIndex] = rf.getRealIndex(int64(curr))
                    break
                }
            }
        }
    }
}

// 判断 Leader 时, 由自认为是 Leader 的节点调用
// 与节点中的大多数节点交换心跳，保证 Leader 是最新的
func (rf *Raft) swapHeartbeat() bool {
    rf.mu.Lock()
    var count int32
    resCh := make(chan bool)
    for i := range rf.peers {
        if i == rf.me {
            rf.nextIndex[i] = rf.getRealLogLen()
            rf.matchIndex[i] = rf.getRealLogLen() - 1
            continue
        }
        // 具体处理逻辑
        go rf.swapHeartbeatWith(i, &count, resCh, len(rf.peers) / 2)
    }
    rf.mu.Unlock()
    // 这里不能阻塞
    // 设置阻塞超时时间
    select {
    case <-time.After(5000 * time.Millisecond):
        return false
    case <-resCh:
        return true
    }
}

// 只发送心跳请求, 即 entries 为空的 AppendEntriesArgs
func (rf *Raft) swapHeartbeatWith(peerAddr string, count *int32, resCh chan bool, needLen int) {
    rf.mu.Lock()
    if rf.state != Leader {
        rf.mu.Unlock()
        return
    }
    // 可能需要发送 snapshot
    if rf.nextIndex[peerAddr] <= rf.lastIncludedIndex {
        rf.doInstallSnapshotTo(peerAddr)
        return
    }
    args := new(AppendEntriesArgs)
    args.Term = rf.currTerm
    args.LeaderId = rf.me
    args.LeaderCommitIndex = rf.commitIndex
    // 每个节点有不同的数据
    args.PrevLogIndex = rf.nextIndex[peerAddr] - 1
    args.PrevLogTerm = rf.logs[rf.getCurrIndex(args.PrevLogIndex)].Term
    reply := new(AppendEntriesReply)
    // 由 nextIndex 和 matchIndex 判断是发送 Heartbeat 还是具体的 AppendEntries
    // 处理 AppendEntriesArgs 中的 Entries
    if args.PrevLogIndex != rf.matchIndex[peerAddr] || rf.nextIndex[peerAddr] >= rf.getRealLogLen() {
        args.Entries = []*LogEntry{}
    } else {
        args.Entries = rf.logs[rf.getCurrIndex(rf.nextIndex[peerAddr]):]
    }
    if len(args.Entries) > 0 {
        DPrintf("[SendAppendEntries]%v[%v] to %v, args.PrevLogIndex %v, len(args.Entries) %v", rf.me, rf.currTerm, peerAddr, args.PrevLogIndex, len(args.Entries))
    } else {
        DPrintf("[SendHeartbeat]%v[%v] to %v", rf.me, rf.currTerm, peerAddr)
    }
    rf.mu.Unlock()

    // 进行 RPC 调用
    err := rf.sendAppendEntries(peerAddr, context.Background(), args, reply)
    if err != nil {
        log.Println(err)
    }

    rf.mu.Lock()
    defer rf.mu.Unlock()
    // fail because of out-dated term
    // 收到更大的 Term，Leader 退位
    if reply.Term > rf.currTerm {
        rf.currTerm = reply.Term
        rf.changeStateTo(Follower)
        // 重置选举 timer
        rf.electionTimer.Reset(getRandElectionTimeout())
        rf.saveState()
        handleHeartbeatRes(count, resCh, needLen)
        return
    }
    // 处理回调结果
    // rf Term 与 args Term 不一致，说明是过期的 reply
    if rf.currTerm != args.Term || rf.state != Leader {
        handleHeartbeatRes(count, resCh, needLen)
        return
    }
    // 维护 log consistency
    rf.maintainLogConsistency(peerAddr, args, reply)
    handleHeartbeatRes(count, resCh, needLen)
}

func handleHeartbeatRes(count *int32, resCh chan bool, needLen int) {
    curr := atomic.AddInt32(count, 1)
    // 交换半数以上心跳则将结果回传
    if int(curr) == needLen {
        resCh <- true
    }
}
