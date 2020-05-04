package raftdb

import "fmt"

func (client *DefaultClient) getDBClient(target string) (RaftDBServiceClient, error) {
    res, ok := client.servers[target]
    if !ok {
        return nil, fmt.Errorf("no connection to raftdb is available: %s", target)
    }
    return *res, nil
}
