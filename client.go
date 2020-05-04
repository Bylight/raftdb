package raftdb

import (
    "context"
    "fmt"
    "google.golang.org/grpc"
    "log"
    "sync/atomic"
)

type Client interface {
    Get(key []byte) (value []byte, err error)
    Put(key, value []byte) error
    Delete(key []byte) error
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
    client.cid = addr.Me + DefaultDbServicePort
    client.initRaftDBClients(addr.ClientAddr)
    return client
}

func (client *DefaultClient) Get(key []byte) (value []byte, err error) {
    DPrintf("[Client:Get] key: %s", key)
    curSeq := atomic.AddInt64(&client.seq, 1)
    // 使用 for 循环实现重发 RPC, 保证 RPC 的有效
    for {
        args := &GetArgs{
            Key: key,
            Seq: curSeq,
        }
        server, err := client.getDBClient(client.serverAddr[client.currLeader])
        if err != nil {
            return nil, err
        }
        reply, err := server.Get(context.Background(), args)
        if err != nil {
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