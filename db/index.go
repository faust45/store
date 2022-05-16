package db

import (
	bolt "go.etcd.io/bbolt"
	"log"
	// time "time"
)

type IterFn func(HandlerIterFn) (bool, error)
type HandlerIterFn func([]byte) error
type IndexFn func([]byte) ([]byte, []byte, error)
type IndexWriterFn func([]byte, []byte) error

type Index struct {
	Root       string
	BucketName string
	Name       string
	Fun        IndexFn
	Parse      func([]byte) (Doc, error)
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

func (index Index) Writer(tx *bolt.Tx) (IndexWriterFn, error) {
	return indexWriter(tx, "indexes", index.Name)
}

func indexWriter(tx *bolt.Tx, root, indexName string) (IndexWriterFn, error) {
	broot, err := tx.CreateBucketIfNotExists([]byte(root))
	if err != nil {
		return nil, err
	}

	b, err := broot.CreateBucketIfNotExists([]byte(indexName))
	if err != nil {
		return nil, err
	}

	rbucketName := []byte("reverse/" + indexName)
	rb, err := broot.CreateBucketIfNotExists(rbucketName)
	if err != nil {
		return nil, err
	}

	return func(key, docId []byte) error {
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
	}, nil
}

func (index Index) recentUpdatesIter(tx *bolt.Tx, batchSize int) IterFn {
	b := tx.Bucket([]byte("updates"))
	c := b.Bucket([]byte(index.BucketName)).Cursor()
	bsource := tx.Bucket([]byte(index.BucketName))

	klast := []byte("lastUpdate/" + index.BucketName)
	lastUpdate := b.Get(klast)

	var key, id []byte
	if lastUpdate != nil {
		key, id = c.Seek(lastUpdate)
	} else {
		key, id = c.First()
	}

	return func(fun HandlerIterFn) (bool, error) {
		for i := 1; i != batchSize; i++ {
			if key == nil {
				return true, nil
			}

			data := bsource.Get(id)
			err := fun(data)
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
	var key, value []byte
	var err error

	windex, err := index.Writer(tx)
	iter := index.recentUpdatesIter(tx, batchSize)

	return iter(func(data []byte) error {
		if data != nil {
			key, value, err = index.Fun(data)
			if err != nil {
				log.Printf("Err index Fun %s %s", err, data)
				return err
			}

			return windex(key, value)
		}

		return nil
	})
}
