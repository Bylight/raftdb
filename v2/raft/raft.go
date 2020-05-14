package raft

import (
    "bytes"
    "encoding/gob"
    "log"
    "sync"
    "sync/atomic"
    "time"
)

type ApplyMsg struct {
    CmdValid bool
    Cmd interface{}
    CmdIndex int64
    Snapshot []byte
}

// Raft 定义一个 Raft 节点
type Raft struct {
    mu sync.Mutex
    peers map[string]*RaftServiceClient // 每个 raft peer 都是一个 dbserver，使用 "ipAddr" 作为key
    me string // this peer's index into peers[]
    persist *Persist
    state State
    dead int32
    // notify channel
    killCh chan bool
    applyCh chan ApplyMsg
    // timer
    heartbeatTimer *time.Timer
    electionTimer *time.Timer
    // Persistent state
    currTerm int64
    votedFor string
    logs []*LogEntry
    // Volatile state
    commitIndex int64
    lastApplied int64
    // leader's state
    // nextIndex []int64
    // matchIndex []int64
    nextIndex map[string]int64
    matchIndex map[string]int64
    // for Snapshot
    lastIncludedIndex int64
    lastIncludedTerm int64
}

// 获取 peer 当前状态
func (rf *Raft) GetState() (currTerm int64, state State) {
    rf.mu.Lock()
    currTerm = rf.currTerm
    state = rf.state
    rf.mu.Unlock()
    return currTerm, state
}

func (rf *Raft) GetIsLeader() bool {
    rf.mu.Lock()
    defer rf.mu.Unlock()
    if rf.state != Leader {
        return false
    }
    // 交换心跳，保证节点信息是最新的
    return rf.swapHeartbeat() && rf.state == Leader
}

// 尝试令当前节点接受一个来自 Client 的 cmd
func (rf *Raft) Start(cmd interface{}) (index int64, term int64, isLeader bool) {
    index = -1
    term = -1
    rf.mu.Lock()
    defer rf.mu.Unlock()
    isLeader = rf.state == Leader
    // 只有 Leader 能够接受来自 Client 的 cmd
    if !isLeader {
        return index, term, isLeader
    }
    rec, ok := cmd.([]byte)
    if !ok {
        log.Fatalf("[ErrStatrCmd] should start []bytes, receive %v", rec)
        return index, term, isLeader
    }
    // Leader 则将 cmd 新增至 logs
    index = rf.getRealLogLen()
    term = rf.currTerm
    // 压缩 cmd
    entry := &LogEntry{
        Term: term,
        Cmd:  rec,
    }
    rf.logs = append(rf.logs, entry)
    DPrintf("[StartInRaft] start new cmd in index %v", index)

    rf.saveState()
    // rf.doAppendEntries()
    return index, term, isLeader
}

// 持久化 raft 节点的 persist state
// 由调用者加锁
func (rf *Raft) saveState() {
    w := new(bytes.Buffer)
    enc := gob.NewEncoder(w)

    logs := rf.logs
    err := enc.Encode(logs)
    if err != nil {
        log.Printf("[EncodingError]: peer[%v][%v] encode logs error[%v]", rf.me, rf.currTerm, err)
    }
    currTerm := rf.currTerm
    err = enc.Encode(currTerm)
    if err != nil {
        log.Fatalf("[EncodingError]: peer[%v][%v] encode currTerm error[%v]", rf.me, rf.currTerm, err)
    }
    votedFor := rf.votedFor
    err = enc.Encode(votedFor)
    if err != nil {
        log.Printf("[EncodingError]: peer[%v][%v] encode votedFor error[%v]", rf.me, rf.currTerm, err)
    }
    lastIncludedIndex := rf.lastIncludedIndex
    err = enc.Encode(lastIncludedIndex)
    if err != nil {
        log.Printf("[EncodingError]: peer[%v][%v] encode lastIncludedIndex error[%v]", rf.me, rf.currTerm, err)
    }
    lastIncludedTerm := rf.lastIncludedTerm
    err = enc.Encode(lastIncludedTerm)
    if err != nil {
        log.Printf("[EncodingError]: peer[%v][%v] encode lastIncludedTerm error[%v]", rf.me, rf.currTerm, err)
    }

    data := w.Bytes()
    rf.persist.SaveRaftState(data)
}

// 存储 raft 节点的 persist state 和 snapshot
// 由调用者加锁
func (rf *Raft) saveStateAndSnapshot(snapshot []byte) {
    state := rf.persist.ReadRaftState()
    rf.persist.SaveStateAndSnapshot(state, snapshot)
}

// 从持久化的 persist state 中恢复
// 由调用者加锁
func (rf *Raft) readPersistState(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }

    r := bytes.NewBuffer(data)
    dec := gob.NewDecoder(r)

    var logs []*LogEntry
    var currTerm int64
    var votedFor string
    var lastIncludedIndex int64
    var lastIncludedTerm int64
    if dec.Decode(&logs) != nil || dec.Decode(&currTerm) != nil || dec.Decode(&votedFor) != nil ||
        dec.Decode(&lastIncludedIndex) != nil || dec.Decode(&lastIncludedTerm) != nil {
        log.Printf("[DecodingError]: peer[%v][%v] decode error", rf.me, rf.currTerm)
    } else {
        rf.logs = logs
        rf.currTerm = currTerm
        rf.votedFor = votedFor
        rf.lastIncludedIndex = lastIncludedIndex
        rf.lastIncludedTerm = lastIncludedTerm
    }
}

// applyEntry apply 一个新 committed 的 entry
// 用于在 commitIndex 发生变化时手动触发
// 由调用者加锁
func (rf *Raft) doApplyEntry() {
    currApply := rf.getCurrIndex(rf.lastApplied)
    for rf.lastApplied < rf.commitIndex {
        rf.lastApplied++
        currApply++
        cmd := rf.logs[currApply].Cmd
        rf.applyCh <- ApplyMsg{
            CmdValid: true,
            Cmd:      cmd,
            CmdIndex: rf.lastApplied,
        }
        DPrintf("[ApplyInRaft] Apply new cmd in index %v", rf.lastApplied)
    }
}

// notifyRestore 主动通知 dbserver 恢复为 snapshot 状态
// 在落后 Follower 收到来自 Leader 的 snapshot 时被触发
// 由调用者加锁
func (rf *Raft) notifyRestore() {
    DPrintf("[NotifyRestore] peer %v", rf.me)
    snapshot := rf.persist.ReadSnapshot()
    rf.applyCh <- ApplyMsg{
        CmdValid: false,
        Cmd:      nil,
        CmdIndex: -1,
        Snapshot: snapshot,
    }
}

// 转换节点至目标状态
// 由调用者加锁
func (rf *Raft) changeStateTo(targetState State) {
    if rf.state == targetState {
        return
    }
    rf.state = targetState
    switch targetState {
    case Follower:
        rf.votedFor = NullVotedFor // 空字符串表示还没投票
        rf.heartbeatTimer.Stop() // Follower 不能发送心跳
    case Candidate:
        rf.doRequestVote() // 成为 Candidate 后立刻选举
        rf.electionTimer.Reset(getRandElectionTimeout()) // Leader 不会直接转为 Candidate，不需要停止心跳计时
    case Leader:
        rf.beLeader()
    }
}

func (rf *Raft) beLeader() {
    DPrintf("[BeLeader] peer %v[%v]", rf.me, rf.currTerm)
    rf.doAppendEntries()
    rf.heartbeatTimer.Reset(HeartbeatRPCTimeout * time.Millisecond)
    // 初始化 Volatile State on Leader
    for i := range rf.peers {
        rf.nextIndex[i] = rf.getRealLogLen()
        rf.matchIndex[i] = 0
    }
}

func (rf *Raft) Kill() {
    atomic.StoreInt32(&rf.dead, 1)
    safeSend(rf.killCh)
    close(rf.killCh)
}

func (rf *Raft) Killed() bool {
    z := atomic.LoadInt32(&rf.dead)
    return z == 1
}

// Make 创建一个新的 raft 节点，供 dbserver 调用
func Make(peers map[string]*RaftServiceClient, me string,
    persist *Persist, applyCh chan ApplyMsg) *Raft {
    DPrintf("[MakeRaft] %v", me)
    rf := &Raft{}
    rf.peers = peers
    rf.persist = persist
    rf.me = me
    rf.state = Follower
    rf.dead = 0

    // notify channel
    rf.applyCh = applyCh
    rf.killCh = make(chan bool, 1)
    // timer
    rf.heartbeatTimer = time.NewTimer(HeartbeatRPCTimeout * time.Millisecond)
    rf.electionTimer = time.NewTimer(getRandElectionTimeout())
    // Persistent state
    rf.currTerm = 0
    rf.votedFor = NullVotedFor
    rf.logs = []*LogEntry{{ Cmd: nil, Term: 0 }} // 生成一个默认的 dummy Entry
    // Volatile state
    rf.commitIndex = 0
    rf.lastApplied = 0
    // leader's state
    rf.nextIndex = make(map[string]int64, len(peers))
    rf.matchIndex = make(map[string]int64, len(peers))

    // 后台周期性进行心跳检查和选举超时检查
    go rf.periodicRequestVote()
    go rf.periodicAppendEntries()

    // snapshot
    rf.lastIncludedIndex = 0
    rf.lastIncludedTerm = 0

    // initialize from state persisted before a crash
    rf.mu.Lock()
    rf.readPersistState(persist.ReadRaftState())
    rf.mu.Unlock()

    // 要根据 snapshot 来决定 lastApplied
    rf.lastApplied = rf.getRealIndex(rf.lastApplied)
    rf.commitIndex = rf.getRealIndex(rf.commitIndex)

    return rf
}

// periodicRequestVote 周期性尝试发送 RequestVote RPC 来进行选举
func (rf *Raft) periodicRequestVote() {
    for {
        select {
        case <-rf.killCh:
            return
        // start election
        case <-rf.electionTimer.C:
            rf.mu.Lock()
            switch rf.state {
            case Follower:
                rf.changeStateTo(Candidate)
            case Candidate:
                rf.doRequestVote()
                rf.electionTimer.Reset(getRandElectionTimeout())
            case Leader:
                rf.electionTimer.Reset(getRandElectionTimeout())
            }
            rf.mu.Unlock()
        }
    }
}

// periodicAppendEntries 周期性尝试发送 AppendEntries RPC 来复制日志或发送心跳
func (rf *Raft) periodicAppendEntries()  {
    for {
        select {
        case <-rf.killCh:
            return
        // send heartbeat in parallel
        case <-rf.heartbeatTimer.C:
            rf.mu.Lock()
            if rf.state == Leader {
                rf.doAppendEntries()
                rf.heartbeatTimer.Reset(HeartbeatRPCTimeout * time.Millisecond)
            }
            rf.mu.Unlock()
        }
    }
}
