package main

import (
	// "menubot/coll"
	// "errors"

	// "strings"
	// "bytes"
	// "encoding/binary"
	"encoding/json"
	"fmt"
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

func main() {
	log.SetOutput(os.Stdout)
	byDate := func(data []byte) ([]byte, []byte, error) {
		var a Appointment
		err := json.Unmarshal(data, &a)
		log.Printf("in byDate %s %s", a)

		return []byte(a.Name), a.ID().Bytes(), err
	}

	collections := []string{"appointments", "users"}
	indexes := []db.Index{
		db.Index{
			BucketName: "appointments",
			Name:       "byDate",
			Fun:        byDate,
			Unique:     false,
		},
	}

	conf := db.Conf{File: "./malta.db", Indexes: indexes, Coll: collections}
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
	fmt.Printf("log: %s\n", err)
}
