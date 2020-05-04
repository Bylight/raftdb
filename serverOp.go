package raftdb

import (
    "context"
    "errors"
    "time"
)

func (dbs *DBServer) Get(ctx context.Context, args *GetArgs) (*GetReply, error) {
    var err error
    reply := new(GetReply)
    // Get 请求是幂等的, 可以重复, 不需要记录 cid
    op := Op{
        Key:   args.Key,
        Seq:   args.Seq,
        Type:  Op_GET,
        Err:   "",
    }
    reply.WrongLeader = true
    bts, err := encodeOp(op)
    if err != nil {
        return nil, err
    }
    index, _, isLeader := dbs.rf.Start(bts)
    if !isLeader {
        return reply, err
    }
    // 等待操作结果
    ch := dbs.getAgreeCh(index)
    select {
    case <- time.After(RpcCallTimeout * time.Millisecond):
        DPrintf("[GetTimeoutInServer] op %v", op)
    case res := <-ch:
        dbs.mu.Lock()
        delete(dbs.agreeChMap, index)
        dbs.mu.Unlock()
        if !isSameOp(op, res) {
            return reply, err
        }
        // 错误要报告给 client
        if res.Err != "" {
            err = errors.New(res.Err)
        }
        reply.Value = res.Value
        // 只有 WrongLeader 为 false, client 才接受这个结果
        reply.WrongLeader = false
    }
    return reply, err
}

func (dbs *DBServer) Put(ctx context.Context, args *PutArgs) (*PutReply, error) {
    var err error
    reply := new(PutReply)
    op := Op{
        Key:   args.Key,
        Value: args.Value,
        Cid:   args.Cid,
        Seq:   args.Seq,
        Err:   "",
        Type:  Op_PUT,
    }
    reply.WrongLeader = true
    bts, err := encodeOp(op)
    if err != nil {
        return nil, err
    }
    index, _, isLeader := dbs.rf.Start(bts)
    if !isLeader {
        return reply, err
    }
    // 等待操作结果
    ch := dbs.getAgreeCh(index)
    select {
    case <- time.After(RpcCallTimeout * time.Millisecond):
        DPrintf("[PutTimeoutInServer] op %v", op)
    case res := <-ch:
        dbs.mu.Lock()
        delete(dbs.agreeChMap, index)
        dbs.mu.Unlock()
        if !isSameOp(op, res) {
            return reply, err
        }
        // 错误要报告给 client
        if res.Err != "" {
            err = errors.New(res.Err)
        }
        // 只有 WrongLeader 为 false, client 才接受这个结果
        reply.WrongLeader = false
    }
    return reply, err
}

func (dbs *DBServer) Delete(ctx context.Context, args *DeleteArgs) (*DeleteReply, error) {
    var err error
    reply := new(DeleteReply)
    op := Op{
        Key:   args.Key,
        Cid:   args.Cid,
        Seq:   args.Seq,
        Err:   "",
        Type:  Op_DELETE,
    }
    reply.WrongLeader = true
    bts, err := encodeOp(op)
    if err != nil {
        return nil, err
    }
    index, _, isLeader := dbs.rf.Start(bts)
    if !isLeader {
        return reply, err
    }
    // 等待操作结果
    ch := dbs.getAgreeCh(index)
    select {
    case <- time.After(RpcCallTimeout * time.Millisecond):
        DPrintf("[DeleteTimeoutInServer] op %v", op)
    case res := <-ch:
        dbs.mu.Lock()
        delete(dbs.agreeChMap, index)
        dbs.mu.Unlock()
        if !isSameOp(op, res) {
            return reply, err
        }
        // 错误要报告给 client
        if res.Err != "" {
            err = errors.New(res.Err)
        }
        // 只有 WrongLeader 为 false, client 才接受这个结果
        reply.WrongLeader = false
    }
    return reply, err
}
