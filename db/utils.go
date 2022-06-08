package db

import (
	"bytes"
	"encoding/binary"
	time "time"
)

func Int64ToBytes(num int64) []byte {
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.BigEndian, num)
	if err != nil {
	}

	return buff.Bytes()
}

func IntToBytes(num int) []byte {
	b := make([]byte, 4)
	b[0] = byte(num)
	b[1] = byte(num >> 8)
	b[2] = byte(num >> 16)
	b[3] = byte(num >> 24)

	return b
}

func BytesToInt(b []byte) int {
	return int(b[0]) | int(b[1])<<8 | int(b[2])<<16 | int(b[3])<<24
}

func GetTimestamp() Timestamp {
	return Timestamp(time.Now().UnixNano())
}

func mapByName(coll []Index) map[string]Index {
	m := make(map[string]Index)
	for _, idx := range coll {
		m[idx.Name] = idx
	}

	return m
}

func mapCollections(coll []string) []Index {
	var indexes []Index
	for _, name := range coll {
		idx := Index{
			root:   "updates",
			Source: name,
			Name:   name,
			Uniq:   true,
		}

		indexes = append(indexes, idx)
	}

	return indexes
}

func bytesToId(b []byte) ID {
	return ID(binary.LittleEndian.Uint64(b))
}
