package db

import (
	// "menubot/coll"
	// "errors"

	// "strings"
	// "bytes"
	"encoding/binary"
	"encoding/json"
	// "strings"
	// "gopkg.in/yaml.v2"
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

type Order int

type Num int

const (
	Desc Order = iota + 1
	Asc
)

type Query struct {
	Index string
	Key   []byte
	Order Order
	Limit int
}

type QueryRange struct {
	Index string
	Scope []byte
	Start []byte
	End   []byte
	Order Order
	Limit int
}

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
			// log.Printf("db.Save: Put data %s\n %s %s", err, doc.ID(), data)
			return err
		}

		return logUpdates(tx, doc)
	})
}

func GenId() ID {
	return ID(time.Now().UnixNano())
}

func Search[T Doc](q QueryRange) []T {
	var docs []T

	index := indexes[q.Index]
	index.Update()

	db.View(func(tx *bolt.Tx) error {
		rindex := indexReader(tx, index, q)
		bsource := tx.Bucket([]byte(index.Source))

		rindex(func(key []byte) error {
			var doc T

			bytes := bsource.Get(key)
			json.Unmarshal(bytes, &doc)

			docs = append(docs, doc)
			return nil
		})

		return nil
	})

	return docs
}

func logUpdates(tx *bolt.Tx, doc Doc) error {
	index := updatesIndexes[doc.BucketName()]
	windex, _ := indexWriter(tx, index)

	k, v := GenId().Bytes(), doc.ID().Bytes()
	return windex([][]byte{k}, v)
}

func indexSetRoot(coll []Index, root string) []Index {
	var arr []Index
	for _, idx := range coll {
		idx.root = root
		arr = append(arr, idx)
	}

	return arr
}

func (a Num) Bytes() []byte {
	return IntToBytes(int(a))
}

func (id ID) Bytes() []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))

	return b
}

func (t Timestamp) Bytes() []byte {
	return Int64ToBytes(int64(t))
}

func Bytes(arr ...Field) []byte {
	var acc []byte
	for _, v := range arr {
		acc = append(acc, v.Bytes()...)
	}

	return acc
}
