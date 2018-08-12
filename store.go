package main

import (
	"io/ioutil"
	"strings"
)

type Store struct {
	rootDir string
}

const dbSuffix = ".db"

func OpenStore(rootDir string) *Store {
	return &Store{rootDir}
}

func (store *Store) listWorkspaces() ([]string, error) {
	files, err := ioutil.ReadDir(store.rootDir)
	if err != nil {
		return nil, err
	}

	var ids []string
	for _, file := range files {
		name := file.Name()
		if strings.HasSuffix(name, dbSuffix) {
			id := name[:len(name)-len(dbSuffix)]
			ids = append(ids, id)
		}
	}
	return ids, nil
}
