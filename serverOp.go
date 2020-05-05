package raftdb

import (
    "context"
    "errors"
    "time"
)

func (dbs *DBServer) Get(ctx context.Context, args *GetArgs) (*GetReply, error) {
    var err error
    reply := new(GetReply)
    // Get 请求是幂等的, 可以重复, 不需要记录 cid; 更新: 但是如果不保证 get 操作的线性执行, 会读到旧的结果
    op := Op{
        Key:   args.Key,
        Seq:   args.Seq,
        Type:  Op_GET,
        Cid: args.Cid,
        Err:   "",
    }
    reply.WrongLeader = true
    bts, err := encodeOp(op)
    if err != nil {
        // 如果编码错误, 则不应继续尝试重发
        reply.WrongLeader = false
        return reply, err
    }
    index, _, isLeader := dbs.rf.Start(bts)
    if !isLeader {
        return reply, err
    }
    // 等待操作结果
    ch := dbs.getAgreeCh(index)
    select {
    case <- time.After(RpcCallTimeout * time.Millisecond):
        DPrintf("[GetTimeoutInServer] op key %s", op.Value)
    case res := <-ch:
        dbs.mu.Lock()
        delete(dbs.agreeChMap, index)
        dbs.mu.Unlock()
        DPrintf("[RecOpResInServer] op %v, isSameOp %v, err %v", &res, isSameOp(op, res), res.Err)
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
        // 如果编码错误, 则不应继续尝试重发
        reply.WrongLeader = false
        return reply, err
    }
    index, _, isLeader := dbs.rf.Start(bts)
    if !isLeader {
        return reply, err
    }
    // 等待操作结果
    ch := dbs.getAgreeCh(index)
    select {
    case <- time.After(RpcCallTimeout * time.Millisecond):
        DPrintf("[PutTimeoutInServer] op key %s value %s", op.Key, op.Value)
    case res := <-ch:
        dbs.mu.Lock()
        delete(dbs.agreeChMap, index)
        dbs.mu.Unlock()
        DPrintf("[RecOpResInServer] op %v, isSameOp %v, err %v", &res, isSameOp(op, res), res.Err)
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
        // 如果编码错误, 则不应继续尝试重发
        reply.WrongLeader = false
        return reply, err
    }
    index, _, isLeader := dbs.rf.Start(bts)
    if !isLeader {
        return reply, err
    }
    // 等待操作结果
    ch := dbs.getAgreeCh(index)
    select {
    case <- time.After(RpcCallTimeout * time.Millisecond):
        DPrintf("[DeleteTimeoutInServer] op key %s", op.Value)
    case res := <-ch:
        dbs.mu.Lock()
        delete(dbs.agreeChMap, index)
        dbs.mu.Unlock()
        DPrintf("[RecOpResInServer] op %v, isSameOp %v, err %v", &res, isSameOp(op, res), res.Err)
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

// 注销一个 client
// 该版本注销 cid2seq
func (dbs *DBServer) Close(ctx context.Context, args *CloseArgs) (*CloseReply, error) {
    var err error
    reply := new(CloseReply)

    // 休眠两个 rpc 超时时间, 保证命令执行完
    time.Sleep(2 * RpcCallTimeout)

    dbs.mu.Lock()
    if _, ok := dbs.cid2seq[args.Cid]; ok {
        delete(dbs.cid2seq, args.Cid)
        reply.Success = true
    }
    dbs.mu.Unlock()
    return reply, err
}
