package db

import (
	bytes "bytes"
	"encoding/json"
	bolt "go.etcd.io/bbolt"
	"testing"
)

type Appointment struct {
	Id       ID
	ClientId ID
	SalonId  ID
	Name     string
	Age      Num
}

func TestMarshalKeys(t *testing.T) {
	keys := [][]byte{[]byte("astra"), []byte("data"), []byte("ritali")}
	data := marshalKeys(keys)
	keysx := unmarshalKeys(data)

	for i, k := range keysx {
		if !bytes.Equal(keys[i], k) {
			t.Fatalf("keys marshal fails: looking for %s but got %s", keys[i], k)
		}

	}
}

func TestShouldCreateBuckets(t *testing.T) {
	collections := []string{"appointments", "users"}
	index := Index{
		Source: "appointments",
		Name:   "byAge",
		Fun:    byAge,
	}
	indexes := []Index{index}
	conf := Conf{
		File:    "./test.db",
		Indexes: indexes,
		Coll:    collections,
	}
	err := Open(conf)
	defer Close()

	if err != nil {
		t.Fatalf("fails to open db %s", err)
	}

	db.Update(func(tx *bolt.Tx) error {
		broot := tx.Bucket([]byte("indexes"))
		if broot == nil {
			t.Fatalf("indexes root doesnt exists")
		}

		b := broot.Bucket([]byte(index.Name))
		if b == nil {
			t.Fatalf("indexes %s bucket doesnt exists", index.Name)
		}

		return nil
	})
}

func TestIndexWriter(t *testing.T) {
	collections := []string{"appointments", "users"}
	index := Index{
		Source: "appointments",
		Name:   "byAge",
		Fun:    byAge,
	}
	conf := Conf{
		File:    "./test.db",
		Indexes: []Index{index},
		Coll:    collections,
	}
	err := Open(conf)
	defer Close()

	if err != nil {
		t.Fatalf("fails to open db %s", err)
	}

	db.Update(func(tx *bolt.Tx) error {
		index := indexes["byAge"]
		updateIndex, _ := indexWriter(tx, index)
		keys := [][]byte{
			[]byte("alisa"),
			[]byte("dali"),
		}

		docId := []byte("1")
		updateIndex(keys, docId)

		keys = [][]byte{
			[]byte("ameli"),
			[]byte("111"),
		}

		updateIndex(keys, docId)
		broot := tx.Bucket([]byte("indexes"))
		b := broot.Bucket([]byte(index.Name))

		if b.Get([]byte("ameli")) == nil {
			t.Fatalf("index looks incorret")
		}

		if b.Get([]byte("111")) == nil {
			t.Fatalf("index looks incorret")
		}

		return nil
	})
}

func byAge(data []byte) ([][]byte, error) {
	var a Appointment
	err := json.Unmarshal(data, &a)
	if err != nil {
		return nil, err
	}

	return [][]byte{
		Bytes(a.SalonId, a.Age),
	}, nil
}
