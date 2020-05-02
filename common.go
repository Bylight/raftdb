package raftdb

type OpType string

const (
    GetOp OpType = "Get"
    PutOp OpType = "Put"
    DeleteOp OpType = "Delete"
)

// server 发送给 Raft 的 cmd
type Op struct {
    Type OpType
    Key []byte
    Value []byte
    Cid string // 记录提出操作请求的 client 地址
    Seq int64 // client 操作请求的序列号，初始为 0，每次加 1
    Err error
}

// 判断两个 Op 是否一致 (之前发送至 raft 的 cmd 可能被新日志覆盖)
func isSameOp(a, b Op) bool {
    res := a.Type == b.Type && a.Seq == b.Seq && isSameBytes(a.Key, b.Key) && a.Cid == b.Cid
    if res && a.Type != GetOp {
        res = res && isSameBytes(a.Value, b.Value)
    }
    return res
}

func isSameBytes(a, b []byte) bool {
    if len(a) != len(b) {
        return false
    }
    if (a == nil) != (b == nil) {
        return false
    }
    b = b[:len(a)] // bounds check 保证后续的 v != b[i] 不会出现越界错误 (BCE 特性)
    for i, v := range a {
        if v != b[i] {
            return false
        }
    }
    return true
}
