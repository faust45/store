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
	// db "github.com/faust45/pasco"
	t "time"

	"os"
)

type Appointment struct {
	Id        db.ID
	ClientId  db.ID
	SalonId   db.ID
	Name      string
	Age       db.Num
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
			Source: "appointments",
			Name:   "byAge",
			Fun:    byAge,
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

	a := Appointment{
		Id:        db.GenId(),
		Name:      "aranavt",
		Age:       15,
		CreatedAt: t.Now()}
	a1 := Appointment{
		Id:        db.GenId(),
		Name:      "fito",
		Age:       19,
		CreatedAt: t.Now()}
	b := Appointment{
		Id:        db.GenId(),
		Name:      "dasha",
		Age:       20,
		CreatedAt: t.Now()}
	f := Appointment{
		Id:        db.GenId(),
		Name:      "mash",
		Age:       21,
		CreatedAt: t.Now()}
	f1 := Appointment{
		Id:        db.GenId(),
		Name:      "Galina",
		Age:       21,
		CreatedAt: t.Now()}
	e := Appointment{
		Id:        db.GenId(),
		Name:      "sasha",
		Age:       45,
		CreatedAt: t.Now()}

	err = db.Save(a)
	err = db.Save(b)
	err = db.Save(f)
	err = db.Save(e)
	err = db.Save(a1)
	err = db.Save(f1)

	// where SalonId == x and Age > 18
	q := db.QueryRange{
		Index: "byAge",
		Scope: []byte("rstrst"),
		Start: db.IntToBytes(49),
		End:   db.IntToBytes(21),
	}

	docs := db.Search[Appointment](q)
	for _, d := range docs {
		log.Printf("%+v %s", d.Age, d.Name)
	}
}

func byAge(data []byte) ([][]byte, error) {
	var a Appointment
	err := json.Unmarshal(data, &a)
	if err != nil {
		return nil, err
	}

	return [][]byte{
		a.Age.Bytes(),
	}, nil
}
