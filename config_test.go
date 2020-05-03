package raftdb

// func TestConfig_DefaultStore(t *testing.T) {
//     config := DefaultConfig()
//     db := config.Db
//     key := []byte("key1")
//     value := []byte("value1")
//
//     err := db.Put(key, value)
//     if err != nil {
//         t.Fatalf("[ErrPut] err %v", err)
//     }
//
//     data, err := db.Get(key)
//     if err != nil || !isSameBytes(data, value) {
//         t.Fatalf("[ErrGet] err %v, return value %v, right value %v", err, data, value)
//     }
//
//     db.Delete(key)
//
//     // err should be "not found"
//     data, err = db.Get(key)
//     if err == nil {
//         t.Fatalf("[ErrGet] err %v, return value %v, right value %v", err, data, "")
//     }
//
//     var err1 error
//     if err1 != nil {
//         t.Fatalf("%v", err1)
//     }
// }
