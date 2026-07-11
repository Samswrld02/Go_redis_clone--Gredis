package db

import "errors"

// basic Gredis db
type Db struct {
	data map[string]string
}

func NewGredisDb() *Db {
	return &Db{data: make(map[string]string)}
}

// Set db data
func (db *Db) Set(key string, value string) (bool, error) {
	db.data[key] = value

	return true, nil
}

// Get cached db data
func (db *Db) Get(key string) (string, error) {
	if value, exists := db.data[key]; exists {
		return value, nil
	}
	return "", errors.New("Key doesn't exist in the database")
}
