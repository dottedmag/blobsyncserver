package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coreos/bbolt"
	"github.com/pkg/errors"
)

type Store struct {
	rootDir    string
	workspaces map[string]*bolt.DB
	mut        sync.Mutex
}

const dbSuffix = ".db"

func OpenStore(rootDir string) *Store {
	return &Store{
		rootDir:    rootDir,
		workspaces: make(map[string]*bolt.DB),
	}
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

func (store *Store) openWorkspace(id string) (*bolt.DB, error) {
	store.mut.Lock()
	defer store.mut.Unlock()

	ws := store.workspaces[id]
	if ws != nil {
		return ws, nil
	}

	err := os.MkdirAll(store.rootDir, 0700)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create directory for workspaces")
	}

	db, err := bolt.Open(filepath.Join(store.rootDir, id+dbSuffix), 0600, &bolt.Options{Timeout: time.Millisecond})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create workspace database")
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err = tx.CreateBucketIfNotExists([]byte("updates"))
		if err != nil {
			return errors.Wrap(err, "unable to create bucket 'updates'")
		}
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create top-level bucket")
	}

	store.workspaces[id] = db
	return db, nil
}
