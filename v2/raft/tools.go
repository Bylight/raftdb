package raft

import (
    "bytes"
    "encoding/gob"
    "fmt"
    "io/ioutil"
    "log"
    "math/rand"
    "sort"
    "time"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug {
        log.Printf(format, a...)
    }
    return
}

// deprecated
// 将 cmd 压缩为 byte 数组
func encodeCmd(cmd interface{}) []byte {
    w := new(bytes.Buffer)
    enc := gob.NewEncoder(w)
    err := enc.Encode(cmd)
    if err != nil {
        log.Printf("[EncodingError]: encode cmd %v error[%v]", cmd, err)
    }
    encCmd := w.Bytes()
    return encCmd
}

// 向缓存为 1 的信道发送一个值，保证其不阻塞
func safeSend(ch chan bool) {
    select {
    case <-ch: // 如果存在，则忽略原先信道中的值
    default:
    }
    ch <- true
}

// deprecated
// 将 byte 数组解码为 cmd
func decodeCmd(data []byte) interface{} {
    if data == nil || len(data) < 1 { // empty data
        return nil
    }
    r := bytes.NewBuffer(data)
    dec := gob.NewDecoder(r)
    var cmd interface{}
    err := dec.Decode(&cmd)
    if err != nil {
        log.Printf("[DecodingError]: decode cmd %v error[%v]", cmd, err)
    }
    return cmd
}

// getRandElectionTimeout 生成随机的选举超时时间
func getRandElectionTimeout() time.Duration {
    rand.Seed(time.Now().UnixNano())
    // electionTimeout := HeartbeatRPCTimeout * 3 + rand.Intn(HeartbeatRPCTimeout) + rand.Intn(HeartbeatRPCTimeout) + rand.Intn(HeartbeatRPCTimeout)
    electionTimeout := HeartbeatRPCTimeout * 3 + rand.Intn(HeartbeatRPCTimeout) + rand.Intn(HeartbeatRPCTimeout)
    timeout := time.Duration(electionTimeout) * time.Millisecond
    return timeout
}

func getMajorityAgreeIndex(matchIndex map[string]int64) int64 {
    tmp := make([]int64, len(matchIndex))
    i := 0
    for _, v := range matchIndex {
        tmp[i] = v
    }
    sort.Slice(tmp, func(i, j int) bool {
        return tmp[i] < tmp[j]
    })
    return tmp[len(tmp) / 2]
}

// 获取 peer 节点实际上应该有的 logs 长度
// 由调用者加锁
func (rf *Raft) getRealLogLen() int64 {
    return int64(len(rf.logs)) + rf.lastIncludedIndex
}

// 获取 peer 节点当前 logs 长度
// 由调用者加锁
func (rf *Raft) getCurrLogLen() int64 {
    return int64(len(rf.logs))
}

// 获取 peer 节点现在的 currIndex 的真实 index
// 由调用者加锁
func (rf *Raft) getRealIndex(currIndex int64) int64 {
    return currIndex + rf.lastIncludedIndex
}

// 获取 peer 节点真实 currIndex 的 currIndex
// 由调用者加锁
func (rf *Raft) getCurrIndex(realIndex int64) int64 {
    return realIndex - rf.lastIncludedIndex
}

// Leader 节点用于尝试更新其 commitIndex
// 由调用者加锁
func (rf *Raft) updateCommitIndex() {
    majorityMatchIndex := getMajorityAgreeIndex(rf.matchIndex)
    DPrintf("[TryToUpdateCommitIndex] leader %v[%v], commitIndex %v, majority %v", rf.me, rf.currTerm, rf.commitIndex, majorityMatchIndex)
    if majorityMatchIndex > rf.commitIndex && rf.logs[rf.getCurrIndex(majorityMatchIndex)].Term == rf.currTerm {
        DPrintf("[CommitIndexUpdated] peer %v[%v], state %s, commitIndex from %v to %v", rf.me, rf.currTerm, rf.state, rf.commitIndex, majorityMatchIndex)
        rf.commitIndex = majorityMatchIndex
        rf.doApplyEntry()
    } else {
        DPrintf("[FailUpdateCommitIndex] majority's term %v, rf.term %v", rf.logs[rf.getCurrIndex(majorityMatchIndex)].Term, rf.currTerm)
    }
}

func (rf *Raft) getPeerClient(target string) (RaftServiceClient, error) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    client, ok := rf.peers[target]
    if !ok {
        return nil, fmt.Errorf("no connection to peer is available: %s", target)
    }
    return *client, nil
}

func (x *AppendEntriesReply) CopyToRaft(y *AppendEntriesReply) {
    y.Success = x.Success
    y.ConflictIndex = x.ConflictIndex
    y.ConflictTerm = x.ConflictTerm
    y.Term = x.Term
}

func (x *RequestVoteReply) CopyToRaft(y *RequestVoteReply) {
    y.Term = x.Term
    y.Granted = x.Granted
}

func (x *InstallSnapshotReply) CopyToRaft(y *InstallSnapshotReply) {
    y.Term = x.Term
}

func WriteFile(file string, content []byte) error {
    err := ioutil.WriteFile(file, content, 0644)
    if err != nil {
        log.Fatalf("[ErrInWriteFile]: %v", err)
    }
    return err
}

func ReadFile(file string) ([]byte, error) {
    content, err := ioutil.ReadFile(file)
    if err != nil {
        log.Fatalf("[ErrInReadFile]: %v", err)
        return nil, err
    }
    return content, nil
}
