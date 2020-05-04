package raftdb

import (
    "fmt"
    "google.golang.org/grpc"
    "sync/atomic"
)

type Client interface {
    InitDBClient(servers []string)
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
    servers map[string]*RaftDBServiceClient // raftdb 的客户端
    serverAddr []string
}

// 供外部接口调用, 返回一个可用的 DefaultClient
func GetDefaultClient() *DefaultClient {
    client := new(DefaultClient)
    addr := PeerAddr{
        Me:         "192.168.1.5",
        ClientAddr: []string{
            "172.16.41.138",
            "192.168.1.5",
        },
    }
    client.serverAddr = addr.ClientAddr
    client.currLeader = 0
    client.cid = addr.Me + ":" + DefaultDbServicePort
    client.initRaftDBClients(addr.ClientAddr)
    return client
}

func (client *DefaultClient) InitDBClient(addr []string) {
    client.InitDBClient(addr)
}

func (client *DefaultClient) Get(key []byte) (value []byte, err error) {
    DPrintf("[Client:Get] key: %v", key)
    curSeq := atomic.AddInt64(&client.seq, 1)
    // 使用 for 循环实现重发 RPC, 保证 RPC 的有效
    for {
        args := &GetArgs{
            Key: key,
            Seq: curSeq,
        }
        server, err := client.getDBClient(client.serverAddr[client.currLeader])
        if err != nil {
            panic(err.Error())
        }
        reply, err := server.Get(nil, args)
        // client 只能向 leader 发送请求
        // 这里采用轮询方式选择 leader
        if reply.WrongLeader {
            client.currLeader = (client.currLeader + 1) % len(client.servers)
            continue
        }
        if err != nil {
            return reply.Value, err
        }
        return reply.Value, err
    }
}
func (client *DefaultClient) Put(key, value []byte) error {
    DPrintf("[Client:Put] key: %v", key)
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
            panic(err.Error())
        }
        reply, err := server.Put(nil, args)
        // client 只能向 leader 发送请求
        // 这里采用轮询方式选择 leader
        if reply.WrongLeader {
            client.currLeader = (client.currLeader + 1) % len(client.servers)
            continue
        }
        if err != nil {
            return err
        }
        return err
    }
}

func (client *DefaultClient) Delete(key []byte) error {
    DPrintf("[Client:Delete] key: %v", key)
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
            panic(err.Error())
        }
        reply, err := server.Delete(nil, args)
        // client 只能向 leader 发送请求
        // 这里采用轮询方式选择 leader
        if reply.WrongLeader {
            client.currLeader = (client.currLeader + 1) % len(client.servers)
            continue
        }
        if err != nil {
            return err
        }
        return err
    }
}

// 初始化各个 raftdb 客户端, 用于向其发起操作请求
func (client *DefaultClient) initRaftDBClients(servers []string) {
    clients := make(map[string]*RaftDBServiceClient)
    for _, v := range servers {
        conn, err := grpc.Dial(v, grpc.WithInsecure())
        if err != nil {
            panic(fmt.Sprintf("[ErrInit] Error in initRaftClients: %v", err))
        }
        client := NewRaftDBServiceClient(conn)
        clients[v + ":" + DefaultDbServicePort] = &client
    }
    client.servers = clients
}