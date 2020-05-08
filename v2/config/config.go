package config

import (
    "github.com/Bylight/raftdb/v2/raft"
    "github.com/Bylight/raftdb/v2/dbserver"
)

type Config interface {
    InitRaftDB()
    initRaftClients()
    initRaftServer(raft *raft.Raft)
    initRaftDBServer(dbServer *dbserver.DBServer)
    initDB() *dbserver.DBServer
}

type PeerAddr struct {
    Me string
    ClientAddr []string
}
