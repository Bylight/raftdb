package raftdb

import (
    "fmt"
    "github.com/Bylight/raftdb/raft"
    "google.golang.org/grpc"
    "log"
    "net"
)

// TODO
const DefaultStoreFile = "./db"
const DefaultRaftServicePort = "7086"
const DefaultDbServicePort = "7087"
const DefaultSnapshotThreshold = 10000

type Config interface {
    InitRaftClients()
    InitRaftServer()
    InitRaftDBServer()
}

// DefaultConfig 用于保存 raftdb 配置
type DefaultConfig struct {
    SnapshotThreshold int
    Clients         map[string]*raft.RaftServiceClient
    RaftServicePort string
    DbServicePort   string
    ConfigDb
    PeerAddr
}

type ConfigDb struct {
    Db Store
    StoreFile string
}

type PeerAddr struct {
    Me string
    ClientAddr []string
}

func GetDefaultConfig(addr PeerAddr) *DefaultConfig {
    db := NewDefaultDB(DefaultStoreFile)
    configDb := ConfigDb{
        Db:        db,
        StoreFile: DefaultStoreFile,
    }
    config := &DefaultConfig{
        SnapshotThreshold: DefaultSnapshotThreshold,
        RaftServicePort:   DefaultRaftServicePort,
        DbServicePort:     DefaultDbServicePort,
        ConfigDb:          configDb,
        PeerAddr:          addr,
    }
    return config
}

// 启动多个 Raft Client
// 只应调用一次
func (config *DefaultConfig) InitRaftClients() {
    if config.Clients != nil {
        log.Fatalln("[ErrInit]Error in initRaftClients: already init clients")
    }
    me := config.Me
    addr := config.PeerAddr
    clients := make(map[string]*raft.RaftServiceClient, len(addr.ClientAddr))
    for _, v := range addr.ClientAddr {
        if v == me {
            continue
        }
        conn, err := grpc.Dial(v, grpc.WithInsecure())
        if err != nil {
            panic(fmt.Sprintf("[ErrInit] Error in initRaftClients: %v", err))
        }
        client := raft.NewRaftServiceClient(conn)
        clients[v + ":" +config.RaftServicePort] = &client
    }
    config.Clients = clients
}

// 启动一个 raft server, 循环响应 raft client 的请求
// 只应调用一次
func (config *DefaultConfig) InitRaftServer() {
    listener, err := net.Listen("tcp", config.RaftServicePort)
    if err != nil {
        panic(fmt.Sprintf("[ErrInit] Error in initRaftServer: %v", err))
    }
    // 创建一个 grpc 服务器对象
    server := grpc.NewServer()
    raft.RegisterRaftServiceServer(server, new(raft.Raft))
    // 开启服务端
    server.Serve(listener)
}

// 启动一个 raftdb server, 循环响应 raftdb client 的请求
// 只应调用一次
func (config *DefaultConfig) InitRaftDBServer() {
    listener, err := net.Listen("tcp", config.DbServicePort)
    if err != nil {
        panic(fmt.Sprintf("[ErrInit] Error in initRaftDBServer: %v", err))
    }
    server := grpc.NewServer()
    RegisterRaftDBServiceServer(server, new(DBServer))
    server.Serve(listener)
}
