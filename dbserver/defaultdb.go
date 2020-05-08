package dbserver

import (
    "bytes"
    "encoding/gob"
    "github.com/syndtr/goleveldb/leveldb"
    "log"
)

// Store 的默认实现，采用 leveldb
type DefaultDB struct {
    db *leveldb.DB
}

func NewDefaultDB(file string) *DefaultDB {
    ddb := new(DefaultDB)
    db, err := leveldb.OpenFile(file, nil)
    if err != nil {
        panic(err)
    }
    ddb.db = db
    return ddb
}

func (ddb *DefaultDB) Close() error {
    return ddb.db.Close()
}

func (ddb *DefaultDB) Get(key []byte) (value []byte, err error) {
    return ddb.db.Get(key, nil)
}

func (ddb *DefaultDB) Put(key, value []byte) error {
    return ddb.db.Put(key, value, nil)
}

func (ddb *DefaultDB) Delete(key []byte) error {
    return ddb.db.Delete(key, nil)
}

func (ddb *DefaultDB) GetSnapshot() ([]byte, error) {
    snapshot, err := ddb.db.GetSnapshot()
    if err != nil {
        return nil, err
    }
    w:= new(bytes.Buffer)
    enc := gob.NewEncoder(w)
    err = enc.Encode(snapshot)
    if err != nil {
        snapshot.Release()
        log.Printf("[DBSnapshotError] db get snapshot error[%v]", err)
        return nil, err
    }
    return w.Bytes(), err
}

// TODO
func (ddb *DefaultDB) RestoreFromSnapshot(data []byte) error {
    if data == nil || len(data) < 1 { // bootstrap without any state?
        log.Printf("[DBError] db restore from empty-snapshot")
        return nil
    }
    r := bytes.NewBuffer(data)
    dec := gob.NewDecoder(r)
    var snapshot leveldb.Snapshot
    if err := dec.Decode(&snapshot); err != nil {
        return err
    }
    // 令 leveldb 从 snapshot 中恢复
    return nil
}
