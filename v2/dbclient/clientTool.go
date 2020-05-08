package dbclient

import (
    "fmt"
    "github.com/Bylight/raftdb/v2/dbRPC"
    "log"
)

var Debug = false

func (client *DefaultClient) getDBClient(target string) (dbRPC.RaftDBServiceClient, error) {
    res, ok := client.servers[target]
    if !ok {
        return nil, fmt.Errorf("no connection to raftdb is available: %s", target)
    }
    return *res, nil
}

func (client *DefaultClient) SetDebug(debug bool) {
    Debug = debug
}

func (leaderCount *safeCurrLeader) safeGet() int {
    leaderCount.mu.Lock()
    defer leaderCount.mu.Unlock()
    return leaderCount.currLeader
}

func (leaderCount *safeCurrLeader) safeAdd(i int) int {
    leaderCount.mu.Lock()
    defer leaderCount.mu.Unlock()
    leaderCount.currLeader += i
    return leaderCount.currLeader
}

func (leaderCount *safeCurrLeader) safeReduce(i int) int {
    leaderCount.mu.Lock()
    defer leaderCount.mu.Unlock()
    leaderCount.currLeader -= i
    return leaderCount.currLeader
}

func (leaderCount *safeCurrLeader) safeSet(i int) int {
    leaderCount.mu.Lock()
    defer leaderCount.mu.Unlock()
    leaderCount.currLeader = i
    return leaderCount.currLeader
}

func debugPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug {
        log.Printf(format, a...)
    }
    return
}
