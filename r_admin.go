package main

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

func (srv *server) adminListWorkspaces(w http.ResponseWriter, r *http.Request) {
	ids, err := srv.store.listWorkspaces()
	if err != nil {
		srv.sendError(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)

	fmt.Fprintf(w, "%d workspaces.\n", len(ids))

	for i, id := range ids {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "#%03d) %s\n", i+1, id)
		fmt.Fprintf(w, "      • curl -X DELETE http://%s/workspaces/%s\n", r.Host, id)
	}
}

func (srv *server) adminDeleteWorkspace(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	err := srv.store.deleteWorkspace(vars["id"])
	if err != nil {
		srv.sendError(w, err)
		return
	}

	http.Error(w, "deleted", http.StatusOK)
}
