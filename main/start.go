package main

import "github.com/Bylight/raftdb"

func main() {
    addr := raftdb.PeerAddr{
        Me:         "192.168.1.5",
        ClientAddr: []string{
            "172.16.41.138",
            "192.168.1.5",
        },
    }
    config := raftdb.GetDefaultConfig(addr)
    config.InitRaftDB()
}

