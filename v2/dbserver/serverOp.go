package dbserver

import (
    "context"
    "errors"
    "github.com/Bylight/raftdb/v2/dbRPC"
    "time"
)

// Get 是只读请求, 无需写入日志, 在确认自己是 Leader 的情况下
func (dbs *DBServer) Get(ctx context.Context, args *dbRPC.GetArgs) (*dbRPC.GetReply, error) {
    var err error
    reply := new(dbRPC.GetReply)
    // Get 请求是幂等的, 可以重复, 不需要记录 cid; 更新: 但是如果不保证 get 操作的线性执行, 会读到旧的结果
    op := dbRPC.Op{
        Key:  args.Key,
        Type: dbRPC.Op_GET,
        Cid:  args.Cid,
        Err:  "",
    }
    reply.WrongLeader = true
    // Leader 才能保证数据是最新的
    if isLeader := dbs.rf.GetIsLeader() ;!isLeader {
        return reply, err
    }
    // Get 操作应该立即执行
    dbs.doOperation(&op)
    DPrintf("[RecOpResInServer] op %v", &op)
    // 不应检测是否重复
    // 错误要报告给 dbclient
    if op.Err != "" {
        err = errors.New(op.Err)
    }
    reply.Value = op.Value
    // 只有 WrongLeader 为 false, dbclient 才接受这个结果
    reply.WrongLeader = false
    return reply, err
}

func (dbs *DBServer) Put(ctx context.Context, args *dbRPC.PutArgs) (*dbRPC.PutReply, error) {
    var err error
    reply := new(dbRPC.PutReply)
    op := dbRPC.Op{
        Key:   args.Key,
        Value: args.Value,
        Cid:   args.Cid,
        Seq:   args.Seq,
        Err:   "",
        Type:  dbRPC.Op_PUT,
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
    // 设置随机超时时间
    timeout := RpcCallTimeout + getRandNum(0, RpcCallTimeout/ 2)
    select {
    case <- time.After(time.Duration(timeout) * time.Millisecond):
        DPrintf("[PutTimeoutInServer] op key %s value %s", op.Key, op.Value)
    case res := <-ch:
        dbs.mu.Lock()
        delete(dbs.agreeChMap, index)
        dbs.mu.Unlock()
        DPrintf("[RecOpResInServer] op %v, isSameOp %v", &res, isSameOp(op, res))
        if !isSameOp(op, res) {
            return reply, err
        }
        // 错误要报告给 dbclient
        if res.Err != "" {
            err = errors.New(res.Err)
        }
        // 只有 WrongLeader 为 false, dbclient 才接受这个结果
        reply.WrongLeader = false
    }
    return reply, err
}

func (dbs *DBServer) Delete(ctx context.Context, args *dbRPC.DeleteArgs) (*dbRPC.DeleteReply, error) {
    var err error
    reply := new(dbRPC.DeleteReply)
    op := dbRPC.Op{
        Key:  args.Key,
        Cid:  args.Cid,
        Seq:  args.Seq,
        Err:  "",
        Type: dbRPC.Op_DELETE,
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
    // 设置随机超时时间
    timeout := RpcCallTimeout + getRandNum(0, RpcCallTimeout/ 2)
    select {
    case <- time.After(time.Duration(timeout) * time.Millisecond):
        DPrintf("[DeleteTimeoutInServer] op key %s", op.Value)
    case res := <-ch:
        dbs.mu.Lock()
        delete(dbs.agreeChMap, index)
        dbs.mu.Unlock()
        DPrintf("[RecOpResInServer] op %v, isSameOp %v", &res, isSameOp(op, res))
        if !isSameOp(op, res) {
            return reply, err
        }
        // 错误要报告给 dbclient
        if res.Err != "" {
            err = errors.New(res.Err)
        }
        // 只有 WrongLeader 为 false, dbclient 才接受这个结果
        reply.WrongLeader = false
    }
    return reply, err
}

// 注销一个 dbclient
// 该版本注销 cid2seq
func (dbs *DBServer) Close(ctx context.Context, args *dbRPC.CloseArgs) (*dbRPC.CloseReply, error) {
    var err error
    reply := new(dbRPC.CloseReply)

    DPrintf("[CloseClient] cid %v", args.Cid)
    // 休眠五个 rpc 超时时间, 保证命令执行完
    time.Sleep(5 * RpcCallTimeout)

    dbs.mu.Lock()
    if _, ok := dbs.cid2seq[args.Cid]; ok {
        delete(dbs.cid2seq, args.Cid)
        reply.Success = true
    }
    dbs.mu.Unlock()
    return reply, err
}
