package main

import (
    "github.com/Bylight/raftdb"
    "log"
)

func main() {
    addr := raftdb.PeerAddr{
        Me:         "106.52.184.128",
        ClientAddr: []string{
            "172.16.41.138",
            "106.52.184.128",
        },
    }
    config := raftdb.GetDefaultConfig(addr)
    config.InitRaftDB()
    log.Println("InitComplete")
}

