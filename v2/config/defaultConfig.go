package config

import (
    "fmt"
    "github.com/Bylight/raftdb/v2/dbRPC"
    "github.com/Bylight/raftdb/v2/raft"
    "github.com/Bylight/raftdb/v2/dbserver"
    "google.golang.org/grpc"
    "log"
    "net"
)

// TODO
const DefaultStoreFile = "./db"
const DefaultRaftServicePort = ":7086"
const DefaultDbServicePort = ":7087"
const DefaultSnapshotThreshold = -1

// DefaultConfig 用于保存 raftdb 配置
type DefaultConfig struct {
    SnapshotThreshold int
    Clients         map[string]*raft.RaftServiceClient
    RaftServicePort string
    DbServicePort   string
    Persist *raft.Persist
    ConfigDb
    PeerAddr
}

type ConfigDb struct {
    Db        dbserver.Store
    StoreFile string
}

func GetDefaultConfig(addr PeerAddr) *DefaultConfig {
    db := dbserver.NewDefaultDB(DefaultStoreFile)
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
        Persist:           raft.MakePersist(),
    }
    return config
}

func (config *DefaultConfig) InitRaftDB() {
    config.initRaftClients()
    dbServer := config.initDB()
    go config.initRaftServer(dbServer.GetRaft())
    config.initRaftDBServer(dbServer)
}

// 启动多个 Raft DefaultClient
// 只应调用一次
func (config *DefaultConfig) initRaftClients() {
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
        conn, err := grpc.Dial(v +DefaultRaftServicePort, grpc.WithInsecure())
        if err != nil {
            panic(fmt.Sprintf("[ErrInit] Error in initRaftClients: %v", err))
        }
        client := raft.NewRaftServiceClient(conn)
        clients[v] = &client
    }
    config.Clients = clients
    log.Println("[InitRaftDB] init raft clients")
}

// 启动一个 raft dbserver, 循环响应 raft dbclient 的请求
// 只应调用一次
func (config *DefaultConfig) initRaftServer(raftServer *raft.Raft) {
    listener, err := net.Listen("tcp", config.RaftServicePort)
    if err != nil {
        panic(fmt.Sprintf("[ErrInit] Error in initRaftServer: %v", err))
    }
    // 创建一个 grpc 服务器对象
    server := grpc.NewServer()
    raft.RegisterRaftServiceServer(server, raftServer)
    // 开启服务端
    log.Println("[InitRaftDB] init raft dbserver")
    server.Serve(listener)
}

// 启动一个 raftdb dbserver, 循环响应 raftdb dbclient 的请求
// 只应调用一次
func (config *DefaultConfig) initRaftDBServer(dbServer *dbserver.DBServer) {
    listener, err := net.Listen("tcp", config.DbServicePort)
    if err != nil {
        panic(fmt.Sprintf("[ErrInit] Error in initRaftDBServer: %v", err))
    }
    server := grpc.NewServer()
    dbRPC.RegisterRaftDBServiceServer(server, dbServer)
    log.Println("[InitRaftDB] init raftDB dbserver")
    server.Serve(listener)
}

// 初始化 DBServer
func (config *DefaultConfig) initDB() *dbserver.DBServer {
    return dbserver.StartDBServer(config.Clients, config.Me, config.Persist, config.SnapshotThreshold, config.Db)
}
