package main

import (
	"log"
	"net/http"

	nrcontext "github.com/getndazn/newrelic-context"
)

func indexHandlerFunc(rw http.ResponseWriter, req *http.Request) {
	rw.Write([]byte("I'm an index page!"))

	client := &http.Client{Timeout: 10}
	nrcontext.WrapHTTPClient(req.Context(), client, func() (*http.Request, error) {
		req, err := http.NewRequest("GET", "http://google.com", nil)

		if err != nil {
			rw.Write([]byte("Can't fetch google :("))
			return nil, err
		}

		return req, nil
	})

	rw.Write([]byte("Google fetched successfully!"))
}

func main() {
	var handler http.Handler

	handler = http.HandlerFunc(indexHandlerFunc)
	nrmiddleware, err := nrcontext.NewMiddleware("test-app", "my-license-key")
	if err != nil {
		log.Print("Can't create newrelic middleware: ", err)
	} else {
		handler = nrmiddleware.Handler(handler)
	}

	http.ListenAndServe(":8000", handler)
}
