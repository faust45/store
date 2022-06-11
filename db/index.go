package db

import (
	bolt "go.etcd.io/bbolt"
	"log"
	// time "time"
	// "math/rand"
	"bytes"
)

type IterFn func(HandlerIterFn) (bool, error)
type HandlerIterFn func([]byte, []byte) error
type IndexFn func([]byte) ([][]byte, error)
type IndexIterFn func(DataIterFn) error
type DataIterFn func([]byte) error
type IndexWriterFn func([][]byte, []byte) error
type IndexCleanerFn func([]byte) error

type Field interface {
	Bytes() []byte
}

type Index struct {
	root   string
	Source string
	Name   string
	Fun    IndexFn
	Uniq   bool
}

type IndexEntry struct {
	Scope [][]byte
	Keys  [][]byte
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

func indexReader(tx *bolt.Tx, index Index, q QueryRange) IndexIterFn {
	broot := tx.Bucket([]byte("indexes"))
	bindex := broot.Bucket([]byte(index.Name))
	c := bindex.Cursor()

	if bytes.Compare(q.Start, q.End) == -1 {
		return iterAsc(q, c)
	} else {
		return iterDesc(q, c)
	}

	// if q.Order == Asc {
	// 	return iterAsc(q, c)
	// } else {
	// 	return iterDesc(q, c)
	// }
}

func iterAsc(q QueryRange, c *bolt.Cursor) IndexIterFn {
	return func(fn DataIterFn) error {
		var k, v []byte
		k, v = c.Seek([]byte(q.Start))

		if k == nil {
			return nil
		}

		for (k != nil && -1 == bytes.Compare(k, q.End)) || 0 == bytes.Compare(k, q.End) {
			fn(v)
			k, v = c.Next()
		}
		// log.Printf("after for desc %d %d", BytesToInt(k), BytesToInt(q.End))
		// log.Printf("desc %d %d", BytesToInt(k), bytes.Compare(k, q.End))

		return nil
	}
}

func iterDesc(q QueryRange, c *bolt.Cursor) IndexIterFn {
	return func(fn DataIterFn) error {
		var k, v []byte
		log.Println("in desc")

		k, v = c.Seek([]byte(q.Start))
		if k == nil {
			k, v = c.Last()
		}

		for k != nil && 1 == bytes.Compare(k, q.End) || 0 == bytes.Compare(k, q.End) {
			fn(v)
			k, v = c.Prev()
		}

		return nil
	}
}

func indexWriter(tx *bolt.Tx, index Index) (IndexWriterFn, IndexCleanerFn) {
	rbucketName := []byte("reverse/" + index.Name)
	broot := tx.Bucket([]byte(index.root))
	b := broot.Bucket([]byte(index.Name))
	rb := broot.Bucket(rbucketName)

	cleaner := func(docId []byte) error {
		bytes := rb.Get(docId)

		if bytes != nil {
			oldKeys := unmarshalKeys(bytes)
			for _, key := range oldKeys {
				err := b.Delete(key)
				if err != nil {
					return err
				}
			}
		}

		return nil
	}

	writer := func(entry [][]byte, docId []byte) error {
		cleaner(docId)

		for _, key := range entry {
			if !index.Uniq {
				key = append(key, docId...)
			}

			err := b.Put(key, docId)
			if err != nil {
				return err
			}
		}

		return rb.Put(docId, marshalKeys(entry))
	}

	return writer, cleaner
}

func marshalKeys(keys [][]byte) []byte {
	buf := new(bytes.Buffer)
	l := IntToBytes(len(keys))
	buf.Write(l)

	for _, b := range keys {
		l = IntToBytes(len(b))
		buf.Write(l)
		buf.Write(b)
	}

	return buf.Bytes()
}

func unmarshalKeys(bytes []byte) [][]byte {
	var keys [][]byte
	c := BytesToInt(bytes[:4])
	counter := 4

	for i := 0; i < c; i++ {
		l := BytesToInt(bytes[counter : counter+4])
		counter += 4
		key := bytes[counter : counter+l]
		keys = append(keys, key)
		counter += l
	}

	return keys
}

// func unmarshalIndexEntry(data []byte) IndexEntry {

// }

// func marshalIndexEntry(data []byte) IndexEntry {

// }

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
	c := b.Bucket([]byte(index.Source)).Cursor()
	bsource := tx.Bucket([]byte(index.Source))

	klast := []byte("lastUpdate/" + index.Source)
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
	updateIndex, cleanIndex := indexWriter(tx, index)
	iter := index.recentUpdatesIter(tx, batchSize)

	return iter(func(id, data []byte) error {
		if data != nil {
			ientry, err := index.Fun(data)
			if err != nil {
				log.Printf("Err index Fun %s %s", err, data)
				return err
			}

			return updateIndex(ientry, id)
		}

		//in case doc deleted
		return cleanIndex(id)
	})
}
