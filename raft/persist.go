package raft

import "sync"

type Persist struct {
    mu        sync.Mutex
    raftState []byte // 保存节点的持久化状态
    snapshot  []byte // 存储 db 快照
}

func MakePersist() *Persist {
    return &Persist{}
}

func (ps *Persist) SaveRaftState(state []byte) {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    ps.raftState = state
}

func (ps *Persist) ReadRaftState() []byte {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    return ps.raftState
}

func (ps *Persist) RaftStateSize() int {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    return len(ps.raftState)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persist) SaveStateAndSnapshot(state []byte, snapshot []byte) {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    ps.raftState = state
    ps.snapshot = snapshot
}

func (ps *Persist) ReadSnapshot() []byte {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    return ps.snapshot
}

func (ps *Persist) SnapshotSize() int {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    return len(ps.snapshot)
}

