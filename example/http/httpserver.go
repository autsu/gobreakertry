package main

import (
	"math/rand"
	"net/http"
	"time"
)

var canVisit bool

func main() {
	rand.Seed(time.Now().Unix())
	go func() {
		for {
			t := rand.Int63n(10)
			select {
			case <-time.After(time.Duration(t) * time.Second):
				canVisit = !canVisit
			}
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if canVisit {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("server error"))
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("success"))
		}
	})
	if err := http.ListenAndServe(":9000", nil); err != nil {
		panic(err)
	}
}
