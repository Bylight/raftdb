package client

type Client interface {
    Get(key []byte) (value []byte, err error)
    Put(key, value []byte) error
    Delete(key []byte) error
    Close()
    initRaftDBClients(servers []string)
}
