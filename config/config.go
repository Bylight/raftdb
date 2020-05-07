package config

import (
    "github.com/Bylight/raftdb/raft"
    "github.com/Bylight/raftdb/server"
)

type Config interface {
    InitRaftDB()
    initRaftClients()
    initRaftServer(raft *raft.Raft)
    initRaftDBServer(dbServer *server.DBServer)
    initDB() *server.DBServer
}
