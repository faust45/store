package db

import (
	bolt "go.etcd.io/bbolt"
	"log"
	// time "time"
	// "encoding/binary"
	// "math/rand"
)

type IterFn func(HandlerIterFn) (bool, error)
type HandlerIterFn func([]byte, []byte) error
type IndexFn func([]byte) ([]byte, error)
type IndexWriterFn func([]byte, []byte) error
type IndexCleanerFn func([]byte) error

type Index struct {
	root       string
	BucketName string
	Name       string
	Fun        IndexFn
}

func (index Index) Update() error {
	log.Printf("updateInBatch start")

	for done := false; !done; {
		var err error
		db.Update(func(tx *bolt.Tx) error {
			done, err = index.batchUpdate(tx, 1000)
			return err
		})
	}

	log.Printf("updateInBatch success")
	return nil
}

func indexWriter(tx *bolt.Tx, index Index) (IndexWriterFn, IndexCleanerFn) {
	rbucketName := []byte("reverse/" + index.Name)
	broot := tx.Bucket([]byte(index.root))
	b := broot.Bucket([]byte(index.Name))
	rb := broot.Bucket(rbucketName)

	cleaner := func(docId []byte) error {
		oldKey := rb.Get(docId)
		err := b.Delete(oldKey)
		if err != nil {
			return err
		}

		return rb.Delete(docId)
	}

	writer := func(key, docId []byte) error {
		key = append(key, docId...)

		oldKey := rb.Get(docId)
		err := b.Delete(oldKey)
		if err != nil {
			return err
		}

		err = b.Put(key, docId)
		if err != nil {
			return err
		}

		return rb.Put(docId, key)
	}

	return writer, cleaner
}

func (index Index) init(tx *bolt.Tx) error {
	broot, err := tx.CreateBucketIfNotExists([]byte(index.root))
	if err != nil {
		return err
	}

	_, err = broot.CreateBucketIfNotExists([]byte(index.Name))
	if err != nil {
		return err
	}

	rbucketName := []byte("reverse/" + index.Name)
	_, err = broot.CreateBucketIfNotExists(rbucketName)
	if err != nil {
		return err
	}

	return nil
}

func (index Index) recentUpdatesIter(tx *bolt.Tx, batchSize int) IterFn {
	b := tx.Bucket([]byte("updates"))
	c := b.Bucket([]byte(index.BucketName)).Cursor()
	bsource := tx.Bucket([]byte(index.BucketName))

	klast := []byte("lastUpdate/" + index.BucketName)
	lastUpdate := b.Get(klast)

	var key, id []byte
	if lastUpdate != nil {
		c.Seek(lastUpdate)
		key, id = c.Next()
	} else {
		key, id = c.First()
	}

	return func(fun HandlerIterFn) (bool, error) {
		for i := 1; i != batchSize; i++ {
			if key == nil {
				return true, nil
			}

			data := bsource.Get(id)
			err := fun(id, data)
			if err != nil {
				return false, err
			}

			err = b.Put(klast, key)
			if err != nil {
				return false, err
			}

			key, id = c.Next()
		}

		return false, nil
	}
}

func (index Index) batchUpdate(tx *bolt.Tx, batchSize int) (bool, error) {
	var key []byte
	var err error

	windex, cindex := indexWriter(tx, index)
	iter := index.recentUpdatesIter(tx, batchSize)

	return iter(func(id, data []byte) error {
		if data != nil {
			key, err = index.Fun(data)
			if err != nil {
				log.Printf("Err index Fun %s %s", err, data)
				return err
			}

			return windex(key, id)
		}

		//in case doc deleted
		return cindex(id)
	})
}
