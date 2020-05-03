package raftdb

type Store interface {
    Get(key []byte) (value []byte, err error)
    Put(key, value []byte) error
    Delete(key []byte) error
    GetSnapshot() (snapshot []byte, err error)
    RestoreFromSnapshot(snapshot []byte) error
    Close() error
}
