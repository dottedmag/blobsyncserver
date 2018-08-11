package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
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
	"github.com/vmihailenco/msgpack"
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

	r.HandleFunc("/workspaces/{id:[0-9a-f]{32}}/changes", srv.get2).Methods("GET").
		Queries("from", "{height:[0-9]+}").Queries("mp", "true")
	r.HandleFunc("/workspaces/{id:[0-9a-f]{32}}/changes", srv.get).Methods("GET").
		Queries("from", "{height:[0-9]+}")
	r.HandleFunc("/workspaces/{id:[0-9a-f]{32}}/changes", srv.post2).Methods("POST").
		Queries("from", "{height:[0-9]+}").Queries("mp", "true")
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

func int64ToWire(id int64) []byte {
	if id < 0 {
		panic("id < 0")
	}
	k := make([]byte, 8)
	binary.BigEndian.PutUint64(k, uint64(id))
	return k
}

func wireToInt64(k []byte) int64 {
	id := binary.BigEndian.Uint64(k)
	if id > math.MaxInt64 {
		panic("id does not fit int64")
	}
	return int64(id)
}

func strToId(s string) (int64, error) {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, err
	}
	if i < 1 {
		return 0, errors.New("id <= 0")
	}
	return i, nil
}

type update struct {
	key   []byte
	value []byte
}

func readUpdates(b *bolt.Bucket, from int64) []update {
	out := make([]update, 0, 64)

	fromKey := int64ToWire(from)

	c := b.Cursor()
	for k, v := c.Seek(fromKey); k != nil; k, v = c.Next() {
		out = append(out, update{k, v})
	}
	return out
}

type update2 struct {
	K int64
	V []byte
}

type updates []update2

func readUpdates2(b *bolt.Bucket, from int64) updates {
	out := make(updates, 0, 64)
	fromKey := int64ToWire(from)

	c := b.Cursor()
	for k, v := c.Seek(fromKey); k != nil; k, v = c.Next() {
		out = append(out, update2{wireToInt64(k), v})
	}
	return out
}

func (s *server) get2(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ws, err := openWorkspace(s, vars["id"])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		// FIXME (dottedmag) do not show internals once in production
		w.Write([]byte(err.Error()))
		return
	}
	from, err := strToId(vars["height"])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = ws.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("updates"))
		if b == nil {
			return errors.New("missing bucket 'updates'")
		}

		next := nextKey(b)
		if from > next+1 {
			return heightMismatchError{next - 1, true}
		}

		updates := readUpdates2(b, from)
		e := msgpack.NewEncoder(w)
		err = e.Encode(updates)
		if err != nil {
			return errors.Wrap(err, "failed to write")
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

func (s *server) get(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ws, err := openWorkspace(s, vars["id"])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		// FIXME (dottedmag) do not show internals once in production
		w.Write([]byte(err.Error()))
		return
	}
	from, err := strToId(vars["height"])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = ws.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("updates"))
		if b == nil {
			return errors.New("missing bucket 'updates'")
		}

		next := nextKey(b)
		if from > next+1 {
			return heightMismatchError{next - 1, true}
		}

		updates := readUpdates(b, from)
		w.Write(int64ToWire(int64(len(updates))))

		for _, update := range updates {
			w.Write(update.key)
			var ob [8]byte
			binary.BigEndian.PutUint64(ob[:], uint64(len(update.value)))
			w.Write(ob[:])
			w.Write(update.value)
		}

		return nil
	})
	if err != nil {
		sendError(w, err)
	}
}

type heightMismatchError struct {
	height int64
	trim   bool // whether client has provided a HIGHER height and needs to trim
}

func (e heightMismatchError) Error() string {
	if e.trim {
		return fmt.Sprintf("trim %d", e.height)
	} else {
		return fmt.Sprintf("conflict %d", e.height)
	}
}

type clientError string

func (s clientError) Error() string {
	return string(s)
}

func nextKey(b *bolt.Bucket) int64 {
	c := b.Cursor()
	k, _ := c.Last()
	if k == nil {
		return 1
	}
	return wireToInt64(k) + 1
}

func (s *server) post2(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	ws, err := openWorkspace(s, vars["id"])
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		// FIXME (dottedmag) do not show internals once in production
		w.Write([]byte(err.Error()))
		return
	}
	height, err := strToId(vars["height"])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	d := msgpack.NewDecoder(r.Body)
	var updates updates
	err = d.Decode(&updates)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(err.Error()))
		return
	}
	err = ws.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("updates"))
		if b == nil {
			return errors.New("missing bucket 'updates'")
		}
		next := nextKey(b)
		if next != height {
			return heightMismatchError{next - 1, (height > next)}
		}
		for i, update := range updates {
			if update.K != next {
				return clientError(fmt.Sprintf("non-sequential id %d, wanted %d in incoming change #%d", update.K, next, i+1))
			}
			err = b.Put(int64ToWire(update.K), update.V)
			next++
		}
		return nil
	})
	if err != nil {
		sendError(w, err)
	}
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
	height, err := strToId(vars["height"])
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	countBin := make([]byte, 8)
	_, err = io.ReadFull(r.Body, countBin)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	count := wireToInt64(countBin)
	err = ws.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("updates"))
		if b == nil {
			return errors.New("missing bucket 'updates'")
		}
		next := nextKey(b)
		if next != height {
			return heightMismatchError{next - 1, (height > next)}
		}
		idbin := make([]byte, 8)
		sizebin := make([]byte, 8)
		var nread int64 = 0
		for {
			_, err := io.ReadFull(r.Body, idbin)
			if err == io.EOF {
				break
			}
			nread++
			if err != nil {
				return clientError("unable to read object id: " +
					err.Error())
			}
			id := wireToInt64(idbin)
			if id != next {
				return clientError(fmt.Sprintf("non-sequential id %d, wanted %d in incoming change #%d", id, next, nread))
			}
			_, err = io.ReadFull(r.Body, sizebin)
			if err != nil {
				return clientError("unable to read object size: " +
					err.Error())
			}
			size := wireToInt64(sizebin)
			val := make([]byte, size)
			_, err = io.ReadFull(r.Body, val)
			if err != nil {
				return clientError("unable to read object: " +
					err.Error())
			}
			err = b.Put(idbin, val)
			if err != nil {
				return errors.Wrap(err, "unable to store object")
			}
			next++
		}
		if nread != count {
			return clientError("way too few objects")
		}
		return nil
	})
	if err != nil {
		sendError(w, err)
	}
}

func sendError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "text/wonderland.inbox.error; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")

	switch err := err.(type) {
	case heightMismatchError:
		w.WriteHeader(http.StatusPreconditionFailed)
		w.Write([]byte(err.Error()))
	case clientError:
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "bad_request %s", err.Error())
	default:
		fmt.Fprintf(os.Stderr, "500: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("server_error"))
	}
	return
}
