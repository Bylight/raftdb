package main

import (
    "github.com/Bylight/raftdb"
    "log"
)

func main() {
    addr := raftdb.PeerAddr{
        Me:         "106.52.184.128", // tencent
        // Me:         "116.62.27.142", // ali
        ClientAddr: []string{
            "116.62.27.142",
            "106.52.184.128",
        },
    }
    config := raftdb.GetDefaultConfig(addr)
    config.InitRaftDB()
    log.Println("InitRaftDB")
}

