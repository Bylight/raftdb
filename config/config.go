package config

import (
    "github.com/Bylight/raftdb/raft"
    "github.com/Bylight/raftdb/dbserver"
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
