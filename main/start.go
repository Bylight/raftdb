package main

import (
    "github.com/Bylight/raftdb/config"
    "log"
)

func main() {
    addr := config.PeerAddr{
        Me:         "106.52.184.128", // tencent
        // Me:         "116.62.27.142", // ali
        // Me:         "119.3.209.146", // huawei
        ClientAddr: []string{
            "116.62.27.142", // ali
            "106.52.184.128", // tencent
            "119.3.209.146", // huawei
        },
    }
    cfg := config.GetDefaultConfig(addr)
    cfg.InitRaftDB()
    log.Println("InitRaftDB")
}

