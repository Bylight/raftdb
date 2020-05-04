package raftdb

import (
    "bytes"
    "encoding/gob"
    "errors"
    "log"
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug {
        log.Printf(format, a...)
    }
    return
}

func safeSendBool(ch chan bool) {
    select {
    case <-ch:
    default:
    }
    ch <- true
}

func safeSendOp(ch chan Op, op Op) {
    select {
    case <-ch:
    default:
    }
    ch <- op
}

// 判断两个 Op 是否一致 (之前发送至 raft 的 cmd 可能被新日志覆盖)
func isSameOp(a, b Op) bool {
    res := a.Type == b.Type && a.Seq == b.Seq && isSameBytes(a.Key, b.Key) && a.Cid == b.Cid
    if res && a.Type == Op_PUT {
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

// 判断命令是否已经执行过
// 由调用者加锁
func (dbs *DBServer) isDuplicatedCmd(cid string, seq int64) bool {
    maxSeq, ok := dbs.cid2seq[cid]
    return ok && seq <= maxSeq
}

// 将 snapshot 转化为二进制编码
func (dbs *DBServer) encodeSnapshot() ([]byte, error) {
    w:= new(bytes.Buffer)
    enc := gob.NewEncoder(w)

    snapshot, err := dbs.db.GetSnapshot()
    if err != nil {
        log.Printf("[DBError] db get snapshot error[%v]", err)
        return nil, err
    }

    dbs.mu.Lock()
    me := dbs.me
    cid2seq := dbs.cid2seq
    dbs.mu.Unlock()

    err = enc.Encode(snapshot)
    if err != nil {
        log.Printf("[EncodingError] server %v encode snapshot error[%v]", me, err)
        return nil, err
    }
    err = enc.Encode(cid2seq)
    if err != nil {
        log.Printf("[EncodingError] server %v encode cid2seq error[%v]", me, err)
        return nil, err
    }

    return w.Bytes(), nil
}

// 将 op 压缩为 byte 数组
func encodeOp(op Op) ([]byte, error) {
    w := new(bytes.Buffer)
    enc := gob.NewEncoder(w)
    err := enc.Encode(op)
    if err != nil {
        log.Printf("[EncodingErrorInServer]: encode op %v error[%v]", op, err)
        return nil, err
    }
    encCmd := w.Bytes()
    return encCmd, nil
}

// 将 byte 数组解码为 op
func decodeOp(data []byte) (Op, error) {
    var op Op
    if data == nil || len(data) < 1 { // empty data
        return op, errors.New("[DecodeOpErrorInServer] decode nil data")
    }
    r := bytes.NewBuffer(data)
    dec := gob.NewDecoder(r)
    err := dec.Decode(&op)
    if err != nil {
        log.Printf("[DecodingError]: decode op %v error[%v]", data, err)
    }
    return op, nil
}
