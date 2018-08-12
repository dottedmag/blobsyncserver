package main

import (
	"fmt"
	"net/http"
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
	fmt.Fprintln(w)

	for _, id := range ids {
		fmt.Fprintf(w, "%s\n", id)
	}
}
