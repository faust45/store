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
	db             *bolt.DB
	indexes        map[string]Index
	updatesIndexes map[string]Index
)

type ID uint64
type Timestamp int64

type Doc interface {
	BucketName() string
	ID() ID
	MarshalJson() ([]byte, error)
}

type Conf struct {
	File    string
	Indexes []Index
	Coll    []string
}

func Open(conf Conf) error {
	var err error
	db, err = bolt.Open(conf.File, 0600, nil)
	if err != nil {
		return err
	}

	updatesColl := mapCollections(conf.Coll)
	indexesColl := indexSetRoot(conf.Indexes, "indexes")

	err = initIndexes(append(indexesColl, updatesColl...))
	if err != nil {
		return err
	}

	updatesIndexes = mapByName(updatesColl)
	indexes = mapByName(indexesColl)

	return nil
}

func initIndexes(coll []Index) error {
	return db.Update(func(tx *bolt.Tx) error {
		for _, idx := range coll {
			if err := idx.init(tx); err != nil {
				return err
			}
		}

		return nil
	})
}

func Close() {
	db.Close()
}

func DB() *bolt.DB {
	return db
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

func mapCollections(coll []string) []Index {
	var indexes []Index
	for _, name := range coll {
		idx := Index{
			root:       "updates",
			BucketName: name,
			Name:       name,
			Unique:     true,
		}

		indexes = append(indexes, idx)
	}

	return indexes
}

func bytesToId(b []byte) ID {
	return ID(binary.LittleEndian.Uint64(b))
}

func logUpdates(tx *bolt.Tx, doc Doc) error {
	index := updatesIndexes[doc.BucketName()]
	windex, err := indexWriter(tx, index)
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

func mapByName(coll []Index) map[string]Index {
	m := make(map[string]Index)
	for _, idx := range coll {
		m[idx.Name] = idx
	}

	return m
}

func indexSetRoot(coll []Index, root string) []Index {
	var arr []Index
	for _, idx := range coll {
		idx.root = root
		arr = append(arr, idx)
	}

	return arr
}
