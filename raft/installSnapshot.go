package raft

import (
    "context"
    "log"
)

// raft server 响应 InstallSnapshot RPC
func (rf *Raft) InstallSnapshot(ctx context.Context, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    reply := new(InstallSnapshotReply)
    DPrintf("[InstallSnapshot] %v[%v] to %v[%v]", args.LeaderId, args.Term, rf.me, rf.currTerm)
    reply.Term = rf.currTerm

    if args.Term < rf.currTerm {
        return reply, nil
    }
    if args.Term > rf.currTerm {
        rf.currTerm = args.Term // update follower's Term
    }
    // 收到该 RPC，便需要转为 Follower
    if rf.state != Follower {
        rf.changeStateTo(Follower)
    }
    // 只要是合法 Leader，就要重置选举 timer
    rf.electionTimer.Reset(getRandElectionTimeout()) // reset electionTimer

    // 防止过期的 InstallSnapshot RPC
    if args.LastIncludedIndex <= rf.lastIncludedIndex {
        return reply, nil
    }

    // 丢弃过期 log
    // 如果当前 log 全部都被压缩，直接重置 log
    if rf.getRealIndex(int64(len(rf.logs)) - 1) <= args.LastIncludedIndex {
        rf.logs = []*LogEntry{{Cmd: nil, Term: args.LastIncludedTerm}}
        // 否则丢弃已经 snapshot 的 log
    } else {
        rf.logs = rf.logs[rf.getCurrIndex(args.LastIncludedIndex):]
    }

    // 更新 snapshot state
    rf.lastIncludedIndex = args.LastIncludedIndex
    rf.lastIncludedTerm = args.LastIncludedTerm
    // rf.lastApplied 也要一同更新，这里可以保证 old lastApplied < lastIncludedIndex, old commitIndex < lastIncludedIndex
    rf.lastApplied = rf.lastIncludedIndex
    rf.commitIndex = rf.lastIncludedIndex
    // 存储 snapshot
    rf.saveStateAndSnapshot(args.Data)
    rf.notifyRestore()

    return reply, nil
}

// raft client 发起 InstallSnapshot RPC
func (rf *Raft) sendInstallSnapshot(server string, ctx context.Context, args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
    client, err := rf.getPeerClient(server)
    if err != nil {
        return err
    }

    res, err := client.InstallSnapshot(ctx, args)
    if err != nil {
        return err
    }

    res.CopyToRaft(reply)
    return nil
}

// doInstallSnapshotTo 向指定节点发送 InstallSnapshot RPC
// 调用前已获取 rf.mu.Lock()
func (rf *Raft) doInstallSnapshotTo(peerAddr string) {
    DPrintf("[SendAppendEntries]%v[%v] to %v", rf.me, rf.currTerm, peerAddr)
    if rf.state != Leader {
        rf.mu.Unlock()
        return
    }
    args := &InstallSnapshotArgs{
        Term:              rf.currTerm,
        LeaderId:          rf.me,
        LastIncludedIndex: rf.lastIncludedIndex,
        LastIncludedTerm:  rf.lastIncludedTerm,
        Data:              rf.persist.ReadSnapshot(),
    }
    rf.mu.Unlock()

    reply := &InstallSnapshotReply{}
    err := rf.sendInstallSnapshot(peerAddr,nil, args, reply)
    if err != nil {
        log.Fatalln(err)
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
    // 无意外，则更新 matchIndex 和 nextIndex
    rf.matchIndex[peerAddr] = rf.lastIncludedIndex
    rf.nextIndex[peerAddr] = rf.lastIncludedIndex + 1
    // 尝试更新 commitIndex
    rf.updateCommitIndex()
}
