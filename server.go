package raftdb

import (
    "bytes"
    "encoding/gob"
    "github.com/Bylight/raftdb/raft"
    "log"
    "sync"
    "sync/atomic"
)

const RpcCallTimeout = 10000
const Debug = true

// server 作为 raftdb 中与 client 进行直接交互的存在，属于一个“中间层”的存在
// 将绑定一个 raft 节点，对于所有来自 DefaultClient 的指令，都会发送到对应 raft 节点；在 raft 已经确认 apply 后，才与实际的数据库进行交互，并将结果返回给 DefaultClient

type DBServer struct {
    mu sync.Mutex
    me string // 对应的 raft 节点地址
    rf *raft.Raft
    persist *raft.Persist
    applyCh chan raft.ApplyMsg
    killCh chan bool
    snapshotThreshold int // 快照阈值
    snapshotCount int
    dead int32

    db Store
    cid2seq map[string]int64 // 记录每个 DefaultClient 发送的最大命令序列号
    agreeChMap map[int64]chan Op // 通知 server 向 raft 发起操作请求
}

// 销毁一个 server
// 待拓展
func (dbs *DBServer) Kill() {
    atomic.StoreInt32(&dbs.dead, 1)
    dbs.rf.Kill()
    dbs.db.Close()
    safeSendBool(dbs.killCh)
    close(dbs.killCh)
}

func (dbs *DBServer) Killed() bool {
    z := atomic.LoadInt32(&dbs.dead)
    return z == 1
}

// 获取一个 server 内部传递 Op 的信道, index 为 raft start 该 Op 后返回的 index
// 收到来自 DefaultClient 的 Op 请求后, 监听该信道
// 直到 server 收到 raft apply 成功的 cmd 并成功执行 Op, 然后从该信道获取操作结果
func (dbs *DBServer) getAgreeCh(index int64) chan Op {
    dbs.mu.Lock()
    defer dbs.mu.Unlock()

    ch, ok := dbs.agreeChMap[index]
    if !ok {
        ch = make(chan Op, 1) // 缓存为 1, 保证不阻塞该信道
        dbs.agreeChMap[index] = ch
    }
    return ch
}

// 作为 goroutine 启动, 在后台循环等待 raft 节点成功 apply 并执行对应操作
func (dbs *DBServer) waitApply() {
    for {
        select {
        case <-dbs.killCh:
            return
        case msg := <-dbs.applyCh:
            if msg.CmdValid {
                dbs.opBaseCmd(msg)
            } else {
                dbs.decodeSnapshot(msg.Snapshot)
            }
        }
    }
}

// server 根据来自 Raft 的 cmd 对数据库发起操作请求
func (dbs *DBServer) opBaseCmd(msg raft.ApplyMsg) {
    bts, ok := msg.Cmd.([]byte)
    if !ok {
        log.Printf("[ErrorCmdTypeInServer] server %v receive illeagl type cmd: %v from raft, should recive []byte", dbs.me, msg.Cmd)
        return
    }
    op, err := decodeOp(bts)
    if err != nil {
        log.Printf(err.Error())
        return
    }
    dbs.doOperation(&op)
    // 每次操作完都要检查是否需要令 raft 进行 log compaction
    go dbs.checkRaftLogCompaction(msg.CmdIndex)
    // 操作结束后，将结果发送至信道，以通知 client 操作结果
    safeSendOp(dbs.getAgreeCh(msg.CmdIndex), op)
}

// 尝试令 Raft 节点进行日志压缩
func (dbs *DBServer) checkRaftLogCompaction(lastIncludedIndex int64) {
    dbs.mu.Lock()
    defer dbs.mu.Unlock()

    // -1 表示不启用日志压缩
    if dbs.snapshotThreshold < 0 {
        return
    }
    snapshotCount := dbs.snapshotCount
    // 抢占锁需要额外时间, 故需要提早进行检测
    if snapshotCount < dbs.snapshotThreshold * 9 / 10 {
        return
    }
    snapshot, err := dbs.encodeSnapshot()
    if err != nil {
        log.Printf("[ErrSnapshotInServer]")
        return
    }
    go dbs.rf.LogCompaction(snapshot, lastIncludedIndex)
}

// 令 server 恢复到 snapshot 的状态
func (dbs *DBServer) decodeSnapshot(data []byte) {
    r := bytes.NewBuffer(data)
    dec := gob.NewDecoder(r)

    var snapshot []byte
    if err := dec.Decode(&snapshot); err != nil {
        log.Printf("[DecodingErr] server %v decode snapshot error[%v]", dbs.me, err)
    }
    var cid2seq map[string]int64
    if err := dec.Decode(&cid2seq); err != nil {
        log.Printf("[DecodingErr] server %v decode cid2seq error[%v]", dbs.me, err)
    }

    dbs.mu.Lock()
    dbs.cid2seq = cid2seq
    dbs.db.RestoreFromSnapshot(snapshot)
    dbs.mu.Unlock()
}

// 对数据库执行具体操作
// 该方法应为唯一能对数据库进行操作的函数
func (dbs *DBServer) doOperation(op *Op) {
    dbs.mu.Lock()
    defer dbs.mu.Unlock()
    var err error
    // 除非 Get 请求，否则只处理最新的请求
    if op.Type == Op_GET || !dbs.isDuplicatedCmd(op.Cid, op.Seq) {
        switch op.Type {
        case Op_GET:
            op.Value, err = dbs.db.Get(op.Key)
        case Op_PUT:
            dbs.cid2seq[op.Cid] = op.Seq
            err = dbs.db.Put(op.Key, op.Value)
        case Op_DELETE:
            dbs.cid2seq[op.Cid] = op.Seq
            err = dbs.db.Delete(op.Key)
        }
        // 每次操作完，count++
        dbs.snapshotCount++

        if err != nil {
            op.Err = err.Error()
            log.Printf("[FailedOpErr] op %v, err %v", op, err)
        } else {
            DPrintf("[OpDoneInServer] type %s, key %s, value %s", op.Type, op.Key, op.Value)
        }
    }
}

// 完成一个 DBSServer 的启动
func startDBServer(
    servers map[string]*raft.RaftServiceClient, me string, persist *raft.Persist, snapshotThreshold int, db Store,
    ) *DBServer {
    dbs := new(DBServer)

    // 来自 config
    dbs.me = me
    dbs.snapshotThreshold = snapshotThreshold
    dbs.db = db
    dbs.persist = persist


    dbs.applyCh = make(chan raft.ApplyMsg)
    dbs.killCh = make(chan bool, 1)
    dbs.cid2seq = make(map[string]int64)
    dbs.agreeChMap = make(map[int64]chan Op)
    go dbs.waitApply()
    if snapshot := persist.ReadSnapshot(); snapshot != nil {
        dbs.decodeSnapshot(snapshot)
    }

    // 来自 config
    dbs.rf = raft.Make(servers, me, persist, dbs.applyCh)

    return dbs
}
