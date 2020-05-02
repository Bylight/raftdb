package raftdb

import (
    "bytes"
    "encoding/gob"
    "github.com/syndtr/goleveldb/leveldb"
    "log"
)

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
        log.Fatalf("[DBError] db get snapshot error[%v]", err)
        return nil, err
    }

    dbs.mu.Lock()
    me := dbs.me
    cid2seq := dbs.cid2seq
    dbs.mu.Unlock()

    err = enc.Encode(snapshot)
    if err != nil {
        log.Fatalf("[EncodingError] server %v encode snapshot error[%v]", me, err)
        return nil, err
    }
    err = enc.Encode(cid2seq)
    if err != nil {
        log.Fatalf("[EncodingError] server %v encode cid2seq error[%v]", me, err)
        return nil, err
    }

    return w.Bytes(), nil
}

// TODO
// 令 server.db 从快照中恢复
func (dbs *DBServer) restoreDb(snapshot *leveldb.Snapshot)  {
}
