package raftdb

import (
    "github.com/Bylight/raftdb/raft"
    "google.golang.org/grpc"
)

// TODO

const DefaultStoreFile = "./db"

// Config 用于保存 raftdb 配置
type Config struct {
    Db Store
    SnapshotThreshold int
    Clients []*raft.RaftServiceClient
    Me string
}

type PeerAddr struct {
    me string
    clientAddr []string
}

func DefaultConfig() *Config {
    db := NewDefaultDB(DefaultStoreFile)
    return &Config{
        Db:                db,
        SnapshotThreshold: 0,
    }
}

// 启动多个 Raft Client
func getRaftServiceClients(addr PeerAddr) []*raft.RaftServiceClient {
    clients := make([]*raft.RaftServiceClient, len(addr.clientAddr))
    for i, v := range addr.clientAddr {
        conn, err := grpc.Dial(v, grpc.WithInsecure())
        if err != nil {
            panic(err)
        }
        client := raft.NewRaftServiceClient(conn)
        clients[i] = &client
    }
    return clients
}

func getRaftServiceServers(addr PeerAddr) {

}

