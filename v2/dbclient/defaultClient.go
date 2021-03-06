package dbclient

import (
    "context"
    "fmt"
    "github.com/Bylight/raftdb/v2/config"
    "github.com/Bylight/raftdb/v2/dbRPC"
    "google.golang.org/grpc"
    "log"
    "sync"
    "sync/atomic"
    "time"
)

// 提供默认的 Client 实现
type DefaultClient struct {
    leader *safeCurrLeader // 当前 Leader 的 ip 地址
    cid string             // ip:port
    seq int64
    servers map[string]*dbRPC.RaftDBServiceClient // raftdb 的客户端, 使用 ipAddr 作为 key
    serverConns map[string]*grpc.ClientConn
    serverAddr []string
}

type safeCurrLeader struct {
    mu sync.Mutex
    currLeader int
}

func GetClient(me string, addrs []string) *DefaultClient {
    addr := config.PeerAddr{
        Me:         me,
        ClientAddr: addrs,
    }
    return GetDefaultClient(addr)
}

// 供外部接口调用, 返回一个可用的 DefaultClient
func GetDefaultClient(addr config.PeerAddr) *DefaultClient {
    client := new(DefaultClient)
    client.serverAddr = addr.ClientAddr
    client.leader = &safeCurrLeader{currLeader: 0}
    // 令 Client 的 id 带上时间戳, 可以保证断开重连的 Client 也能维持序列号最新 (否则可能和断开连接之前冲突)
    client.cid = addr.Me + config.DefaultDbServicePort + fmt.Sprint(time.Now().Unix())
    client.initRaftDBClients(addr.ClientAddr)
    return client
}

// 供外部接口调用, 执行 Get 操作
// 对于不存在的 key, Get 会返回一个空的 value 和一个 not found 的错误
func (client *DefaultClient) Get(key []byte) (value []byte, err error) {
    debugPrintf("[Client:Get] key: %s", key)
    // 使用 for 循环实现重发 RPC, 保证 RPC 的有效
    // 即使出错, 也要尝试遍历各个节点, 这样节点意外宕机时才能保证系统可用
    count := 0
    currLeader := client.leader.safeGet()
    for {
        args := &dbRPC.GetArgs{
            Key: key,
            Cid: client.cid,
        }
        server, err := client.getDBClient(client.serverAddr[currLeader])
        if err != nil {
            return nil, err
        }
        reply, err := server.Get(context.Background(), args)
        count++
        if err != nil {
            debugPrintf("[ErrGetInClient] addr %v, err %v", client.serverAddr[currLeader], err)
            if count < len(client.servers) {
                currLeader = (currLeader + 1) % len(client.servers)
                continue
            }
            return nil, err
        }
        // dbclient 只能向 leader 发送请求
        // 这里采用轮询方式选择 leader
        if reply.WrongLeader {
            currLeader = (currLeader + 1) % len(client.servers)
            continue
        }
        // 成功执行后, 保存 leader
        client.leader.safeSet(currLeader)
        return reply.Value, err
    }
}

// 供外部接口调用, 执行 Put 操作
// 对于存在的 key, Put 覆盖旧的 value
func (client *DefaultClient) Put(key, value []byte) error {
    debugPrintf("[Client:Put] key: %s, value: %s", key, value)
    curSeq := atomic.AddInt64(&client.seq, 1)
    currLeader := client.leader.safeGet()
    // 使用 for 循环实现重发 RPC, 保证 RPC 的有效
    count := 0
    for {
        args := &dbRPC.PutArgs{
            Key:   key,
            Value: value,
            Seq:   curSeq,
            Cid:   client.cid,
        }
        server, err := client.getDBClient(client.serverAddr[currLeader])
        if err != nil {
            return err
        }
        reply, err := server.Put(context.Background(), args)
        count++
        if err != nil {
            debugPrintf("[ErrPutInClient] addr %v, err %v", client.serverAddr[currLeader], err)
            if count < len(client.servers) {
                currLeader = (currLeader + 1) % len(client.servers)
                continue
            }
            return err
        }
        // dbclient 只能向 leader 发送请求
        // 这里采用轮询方式选择 leader
        if reply.WrongLeader {
            currLeader = (currLeader + 1) % len(client.servers)
            continue
        }
        client.leader.safeSet(currLeader)
        return err
    }
}

// 供外部接口调用, 执行 Delete 操作
// 对于不存在的 key, Delete 不会返回错误
func (client *DefaultClient) Delete(key []byte) error {
    debugPrintf("[Client:Delete] key: %s", key)
    curSeq := atomic.AddInt64(&client.seq, 1)
    currLeader := client.leader.safeGet()
    // 使用 for 循环实现重发 RPC, 保证 RPC 的有效
    count := 0
    for {
        args := &dbRPC.DeleteArgs{
            Key: key,
            Seq: curSeq,
            Cid: client.cid,
        }
        server, err := client.getDBClient(client.serverAddr[currLeader])
        if err != nil {
            return err
        }
        reply, err := server.Delete(context.Background(), args)
        count++
        if err != nil {
            debugPrintf("[ErrDeleteInClient] addr %v, err %v", client.serverAddr[currLeader], err)
            if count < len(client.servers) {
                currLeader = (currLeader + 1) % len(client.servers)
                continue
            }
            return err
        }
        // dbclient 只能向 leader 发送请求
        // 这里采用轮询方式选择 leader
        if reply.WrongLeader {
            currLeader = (currLeader + 1) % len(client.servers)
            continue
        }
        client.leader.safeSet(currLeader)
        return err
    }
}

// 供外部接口调用, 主动关闭一个 dbclient
func (client *DefaultClient) Close() {
    // 向所有 dbserver 发送关闭请求
    for i := 0; i < len(client.servers); i++ {
        args := &dbRPC.CloseArgs{Cid: client.cid}
        i = i % len(client.servers)
        server, err := client.getDBClient(client.serverAddr[i])
        if err != nil {
            log.Printf(err.Error())
        }
        _, err = server.Close(context.Background(), args)
        if err != nil {
            log.Printf(err.Error())
        }
        // 主动关闭连接防止连接泄露
        client.serverConns[client.serverAddr[i]].Close()
    }
}

// 初始化各个 raftdb 客户端, 用于向其发起操作请求
func (client *DefaultClient) initRaftDBClients(servers []string) {
    clients := make(map[string]*dbRPC.RaftDBServiceClient)
    conns := make(map[string]*grpc.ClientConn)
    for _, v := range servers {
        conn, err := grpc.Dial(v +config.DefaultDbServicePort, grpc.WithInsecure())
        if err != nil {
            panic(fmt.Sprintf("[ErrInit] Error in initRaftClients: %v", err))
        }
        client := dbRPC.NewRaftDBServiceClient(conn)
        clients[v] = &client
        conns[v] = conn
    }
    client.servers = clients
    client.serverConns = conns
    debugPrintf("[InitInClient] init raftDB dbclient")
}
