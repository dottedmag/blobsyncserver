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
	workspaces map[string]*Workspace
	mut        sync.Mutex
	cond       *sync.Cond
}

type Workspace struct {
	id       string
	DB       *bolt.DB
	useCount int
}

const dbSuffix = ".db"

func OpenStore(rootDir string) *Store {
	store := &Store{
		rootDir:    rootDir,
		workspaces: make(map[string]*Workspace),
	}
	store.cond = sync.NewCond(&store.mut)
	return store
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

func (store *Store) deleteWorkspace(id string) error {
	store.mut.Lock()
	defer store.mut.Unlock()

	for {
		ws := store.workspaces[id]
		if ws == nil {
			break
		}
		if ws.useCount == 0 {
			ws.DB.Close()
			store.workspaces[id] = nil
			break
		}
		store.cond.Wait()
	}

	fn := filepath.Join(store.rootDir, id+dbSuffix)
	err := os.Remove(fn)
    if os.IsNotExist(err) {
        err = nil
    }
	return err
}

func (store *Store) openWorkspace(id string) (*Workspace, error) {
	store.mut.Lock()
	defer store.mut.Unlock()

	ws := store.workspaces[id]
	if ws == nil {
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

		ws = &Workspace{
			id:       id,
			DB:       db,
			useCount: 1,
		}
		store.workspaces[id] = ws
	}

	ws.useCount++
	return ws, nil
}

func (store *Store) releaseWorkspace(ws *Workspace) {
	store.mut.Lock()
	defer store.mut.Unlock()

	if ws.useCount <= 0 {
		panic("releaseWorkspace when useCount == 0")
	}
	ws.useCount--

	store.cond.Broadcast()
}
