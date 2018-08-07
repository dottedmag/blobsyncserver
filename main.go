package main

import (
	"context"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/coreos/bbolt"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

type server struct {
	rootDir    string
	workspaces map[string]*bolt.DB
	mutex      sync.Mutex
}

func main() {
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ltime)

	srv := &server{workspaces: map[string]*bolt.DB{}}

	var port int
	flag.IntVar(&port, "port", 3003, "TCP port to listen on")
	flag.StringVar(&srv.rootDir, "db", "/tmp/blobsyncserver", "Database root folder")
	flag.Parse()

	shutdownChannel := make(chan bool)
	stopChannel := make(chan os.Signal, 1)
	signal.Notify(stopChannel, syscall.SIGINT, syscall.SIGTERM)

	var r = mux.NewRouter()

	r.HandleFunc("/workspaces/{id:[0-9a-f]{32}}/changes", srv.get).Methods("GET").
		Queries("from", "{height:[0-9]+}")
	r.HandleFunc("/workspaces/{id:[0-9a-f]{32}}/changes", srv.post).Methods("POST").
		Queries("from", "{height:[0-9]+}")

	httpSrv := &http.Server{Addr: net.JoinHostPort("", strconv.Itoa(port)), Handler: r}

	go func() {
		<-stopChannel

		httpSrv.SetKeepAlivesEnabled(false)

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if err := httpSrv.Shutdown(ctx); err != nil && err != context.DeadlineExceeded {
			log.Printf("error shutting down HTTP server: %s\n", err.Error())
		} else if err != nil {
			log.Println("timeout shutting down HTTP server, forced")
		} else {
			log.Println("graceful shutdown")
		}

		shutdownChannel <- true
	}()

	log.Printf("Sync server starting on port %d with database at %s\n", port, srv.rootDir)
	err := httpSrv.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		log.Printf("failure starting HTTP server: %s\n", err)
		return
	}

	<-shutdownChannel
}

func openWorkspace(server *server, id string) (*bolt.DB, error) {
	server.mutex.Lock()
	defer server.mutex.Unlock()

	ws := server.workspaces[id]
	if ws != nil {
		return ws, nil
	}

	err := os.MkdirAll(server.rootDir, 0700)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create directory for workspaces")
	}

	db, err := bolt.Open(filepath.Join(server.rootDir, id+".db"), 0600, &bolt.Options{Timeout: time.Millisecond})
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

	server.workspaces[id] = db
	return db, nil
}

func idToKey(id uint64) []byte {
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, id)
	return k
}

type update struct {
	key   []byte
	value []byte
}

func readUpdates(b *bolt.Bucket, from uint64) []update {
	out := make([]update, 0, 64)

	fromKey := idToKey(from)

	c := b.Cursor()
	for k, v := c.Seek(fromKey); k != nil; k, v = c.Next() {
		out = append(out, update{k, v})
	}
	return out
}

func (s *server) get(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ws, err := openWorkspace(s, vars["id"])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		// FIXME (dottedmag) do not show internals once in production
		w.Write([]byte(err.Error()))
		return
	}
	from, err := strconv.ParseUint(vars["height"], 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = ws.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("updates"))
		if b == nil {
			return errors.New("missing bucket 'updates'")
		}

		for _, update := range readUpdates(b, from) {
			w.Write(update.key)
			var ob [8]byte
			binary.BigEndian.PutUint64(ob[:], uint64(len(update.value)))
			w.Write(ob[:])
			w.Write(update.value)
		}

		return nil
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		// FIXME (dottedmag) do not show internals once in production
		w.Write([]byte(err.Error()))
		return
	}
}

type heightMismatchError struct{}

func (heightMismatchError) Error() string {
	return "height mismatch"
}

func (s *server) post(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ws, err := openWorkspace(s, vars["id"])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		// FIXME (dottedmag) do not show internals once in production
		w.Write([]byte(err.Error()))
		return
	}
	height, err := strconv.ParseUint(vars["height"], 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = ws.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("updates"))
		if b == nil {
			return errors.New("missing bucket 'updates'")
		}
		next := b.Sequence()
		if next != height {
			return heightMismatchError{}
		}
		length := make([]byte, 8)
		for {
			_, err := io.ReadFull(r.Body, length)
			if err == io.EOF {
				break
			}
			if err != nil {
				return errors.Wrap(err, "unable to read object size")
			}
			size := binary.BigEndian.Uint64(length)
			val := make([]byte, size)
			_, err = io.ReadFull(r.Body, val)
			if err != nil {
				return errors.Wrap(err, "unable to read object")
			}
			id, _ := b.NextSequence()
			// NextSequence() returns already-incremented sequence number, starting from 1
			key := idToKey(id - 1)
			err = b.Put(key, val)
			if err != nil {
				return errors.Wrap(err, "unable to store object")
			}
		}
		return nil
	})
	if err != nil {
		if _, ok := err.(heightMismatchError); ok {
			w.WriteHeader(http.StatusPreconditionFailed)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			// FIXME (dottedmag) do not show internals once in production
			w.Write([]byte(err.Error()))
		}
		return
	}
}
