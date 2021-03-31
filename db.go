package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/syndtr/goleveldb/leveldb"
)

var levelDb *Store

type Store struct {
	*leveldb.DB
}

func init() {
	db, err := leveldb.OpenFile("db/", nil)
	if err != nil {
		log.Errorf("打开数据库出错 %v", err.Error())
	}
	levelDb = &Store{DB: db}
}
