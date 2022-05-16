package db

import (
	// "menubot/coll"
	// "errors"

	// "strings"
	// "bytes"
	"encoding/binary"
	// "encoding/json"
	// "strings"
	// "gopkg.in/yaml.v2"
	"bytes"
	bolt "go.etcd.io/bbolt"
	"log"
	time "time"
	// "io/ioutil"
	// "net/http"
	// "math/rand"
)

var (
	db      *bolt.DB
	indexes = make(map[string]Index)
)

type ID uint64
type Timestamp int64

type Doc interface {
	BucketName() string
	ID() ID
	MarshalJson() ([]byte, error)
}

func Open(filePath string) error {
	var err error
	db, err = bolt.Open(filePath, 0600, nil)

	return err
}

func Close() {
	db.Close()
}

func DB() *bolt.DB {
	return db
}

func AddIndex(idxs ...Index) {
	for _, idx := range idxs {
		indexes[idx.Name] = idx
	}
}

func Save(doc Doc) error {
	return db.Update(func(tx *bolt.Tx) error {
		bucketName := doc.BucketName()
		b, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		if err != nil {
			return err
		}

		data, err := doc.MarshalJson()
		if err != nil {
			log.Printf("db.Save: Marshal %s", err)
			return err
		}

		key := doc.ID().Bytes()
		err = b.Put(key, data)
		if err != nil {
			log.Printf("db.Save: Put data %s\n %s %s", err, doc.ID(), data)
			return err
		}

		return logUpdates(tx, doc)
	})
}

func GenId() ID {
	return ID(time.Now().UnixNano())
}

func Search(indexName, start, end string) {
	index := indexes[indexName]
	index.Update()
}

func bytesToId(b []byte) ID {
	return ID(binary.LittleEndian.Uint64(b))
}

func logUpdates(tx *bolt.Tx, doc Doc) error {
	windex, err := indexWriter(tx, "updates", doc.BucketName())
	if err != nil {
		return err
	}

	k, v := GenId().Bytes(), doc.ID().Bytes()
	return windex(k, v)
}

func intToBytes(num int64) []byte {
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.BigEndian, num)
	if err != nil {
	}

	return buff.Bytes()
}

func (id ID) Bytes() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))

	return b
}

func GetTimestamp() Timestamp {
	return Timestamp(time.Now().UnixNano())
}

func (t Timestamp) Bytes() []byte {
	return intToBytes(int64(t))
}
