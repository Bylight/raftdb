package raftdb

import (
    "context"
    "fmt"
    "google.golang.org/grpc"
    "log"
    "sync/atomic"
    "time"
)

type Client interface {
    Get(key []byte) (value []byte, err error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Close()
    initRaftDBClients(servers []string)
}

// 提供默认的 Client 实现
type DefaultClient struct {
    currLeader int // 当前 Leader 的 ip 地址
    cid string // ip:port
    seq int64
    servers map[string]*RaftDBServiceClient // raftdb 的客户端, 使用 ipAddr 作为 key
    serverAddr []string
}

// 供外部接口调用, 返回一个可用的 DefaultClient
func GetDefaultClient(addr PeerAddr) *DefaultClient {
    client := new(DefaultClient)
    client.serverAddr = addr.ClientAddr
    client.currLeader = 0
    // TODO
    // 问题: Client 断开重连后，如何保证生成一样的 id, 或是说保持 seq 最新?
    // 当前解决方式, 令 Client 的 id 带上时间戳, 但这种情况下, raftdb 需要定时重启, 防止 cid2seq 占用过多内存
    client.cid = addr.Me + DefaultDbServicePort + fmt.Sprint(time.Now().Unix())
    client.initRaftDBClients(addr.ClientAddr)
    return client
}

// 供外部接口调用, 执行 Get 操作
// 对于不存在的 key, Get 会返回一个空的 value 和一个 not found 的错误
func (client *DefaultClient) Get(key []byte) (value []byte, err error) {
    DPrintf("[Client:Get] key: %s", key)
    curSeq := atomic.AddInt64(&client.seq, 1)
    // 使用 for 循环实现重发 RPC, 保证 RPC 的有效
    for {
        args := &GetArgs{
            Key: key,
            Seq: curSeq,
            Cid: client.cid,
        }
        server, err := client.getDBClient(client.serverAddr[client.currLeader])
        if err != nil {
            return nil, err
        }
        reply, err := server.Get(context.Background(), args)
        if err != nil {
            // 只读操作可直接重试
            if err.Error() == DupReadOnlyOp {
                continue
            }
            DPrintf("[ErrGetInClient] err %v", err)
            return nil, err
        }
        // client 只能向 leader 发送请求
        // 这里采用轮询方式选择 leader
        if reply.WrongLeader {
            client.currLeader = (client.currLeader + 1) % len(client.servers)
            continue
        }
        return reply.Value, err
    }
}

// 供外部接口调用, 执行 Put 操作
// 对于存在的 key, Put 覆盖旧的 value
func (client *DefaultClient) Put(key, value []byte) error {
    DPrintf("[Client:Put] key: %s, value: %s", key, value)
    curSeq := atomic.AddInt64(&client.seq, 1)
    // 使用 for 循环实现重发 RPC, 保证 RPC 的有效
    for {
        args := &PutArgs{
            Key:   key,
            Value: value,
            Seq:   curSeq,
            Cid:   client.cid,
        }
        server, err := client.getDBClient(client.serverAddr[client.currLeader])
        if err != nil {
            return err
        }
        reply, err := server.Put(context.Background(), args)
        if err != nil {
            DPrintf("[ErrPutInClient] err %v", err)
            return err
        }
        // client 只能向 leader 发送请求
        // 这里采用轮询方式选择 leader
        if reply.WrongLeader {
            client.currLeader = (client.currLeader + 1) % len(client.servers)
            continue
        }
        return err
    }
}

// 供外部接口调用, 执行 Delete 操作
// 对于不存在的 key, Delete 不会返回错误
func (client *DefaultClient) Delete(key []byte) error {
    DPrintf("[Client:Delete] key: %s", key)
    curSeq := atomic.AddInt64(&client.seq, 1)
    // 使用 for 循环实现重发 RPC, 保证 RPC 的有效
    for {
        args := &DeleteArgs{
            Key: key,
            Seq: curSeq,
            Cid: client.cid,
        }
        server, err := client.getDBClient(client.serverAddr[client.currLeader])
        if err != nil {
            return err
        }
        reply, err := server.Delete(context.Background(), args)
        if err != nil {
            DPrintf("[ErrDeleteInClient] err %v", err)
            return err
        }
        // client 只能向 leader 发送请求
        // 这里采用轮询方式选择 leader
        if reply.WrongLeader {
            client.currLeader = (client.currLeader + 1) % len(client.servers)
            continue
        }
        return err
    }
}

// 供外部接口调用, 主动关闭一个 client
func (client *DefaultClient) Close() {
    // 向所有 server 发送关闭请求
    for i := 0; i < len(client.servers); i++ {
        args := &CloseArgs{Cid: client.cid}
        i = i % len(client.servers)
        server, err := client.getDBClient(client.serverAddr[i])
        if err != nil {
            log.Printf(err.Error())
        }
        _, err = server.Close(context.Background(), args)
        if err != nil {
            log.Printf(err.Error())
        }
    }
}

// 初始化各个 raftdb 客户端, 用于向其发起操作请求
func (client *DefaultClient) initRaftDBClients(servers []string) {
    clients := make(map[string]*RaftDBServiceClient)
    for _, v := range servers {
        conn, err := grpc.Dial(v + DefaultDbServicePort, grpc.WithInsecure())
        if err != nil {
            panic(fmt.Sprintf("[ErrInit] Error in initRaftClients: %v", err))
        }
        client := NewRaftDBServiceClient(conn)
        clients[v] = &client
    }
    client.servers = clients
    log.Println("[InitInClient] init raftDB client")
}