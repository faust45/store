package main

import (
	// "menubot/coll"
	// "errors"

	// "strings"
	// "bytes"
	// "encoding/binary"
	"encoding/json"
	// "strings"
	// "gopkg.in/yaml.v2"
	"log"
	db "store/db"
	t "time"
	// "io/ioutil"
	// "net/http"
	"os"
)

type Appointment struct {
	Id        db.ID
	ClientId  db.ID
	Name      string
	CreatedAt t.Time
}

func (a Appointment) BucketName() string {
	return "appointments"
}

func (a Appointment) ID() db.ID {
	return a.Id
}

func (a Appointment) MarshalJson() ([]byte, error) {
	b, err := json.Marshal(a)
	return b, err
}

var (
	collections = []string{"appointments", "users"}
	indexes     = []db.Index{
		db.Index{
			BucketName: "appointments",
			Name:       "byDate",
			Fun:        byDate,
		},
	}
	conf = db.Conf{
		File:    "./malta.db",
		Indexes: indexes,
		Coll:    collections,
	}
)

func main() {
	log.SetOutput(os.Stdout)

	err := db.Open(conf)
	defer db.Close()
	if err != nil {
		log.Fatal("Couldn't init db, ", err)
	}

	a := Appointment{Id: db.GenId(), Name: "aranavt", CreatedAt: t.Now()}
	b := Appointment{Id: db.GenId(), Name: "aranavt", CreatedAt: t.Now()}
	err = db.Save(a)
	err = db.Save(b)
	if err != nil {
		log.Printf("Save: %s", err)
	}

	db.Search("byDate", "aranavt", "aranavt")
}

func byDate(data []byte) ([]byte, error) {
	var a Appointment
	log.Printf("in byDate %s", a)

	err := json.Unmarshal(data, &a)
	if err != nil {
		return nil, err
	}

	return []byte(a.Name), nil
}

// if !index.Unique {
// 		postfix := make([]byte, 2)
// 		binary.BigEndian.PutUint16(postfix, uint16(rand.Int()))
// 		key = append(key, postfix...)
// 	}
