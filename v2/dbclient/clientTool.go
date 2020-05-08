package dbclient

import (
    "fmt"
    "github.com/Bylight/raftdb/v2/gRPC"
)

func (client *DefaultClient) getDBClient(target string) (gRPC.RaftDBServiceClient, error) {
    res, ok := client.servers[target]
    if !ok {
        return nil, fmt.Errorf("no connection to raftdb is available: %s", target)
    }
    return *res, nil
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
