package main

import (
	"context"
	"fmt"

	"log"
	"net/http"

	nrcontext "github.com/getndazn/newrelic-context"
)

func Consume(ctx context.Context, query string) {
	client := &http.Client{Timeout: 10}
	nrcontext.WrapHTTPClient(ctx, client, func() (*http.Request, error) {
		req, err := http.NewRequest("GET", fmt.Sprintf("https://www.google.com.vn/?q=%v", query), nil)

		if err != nil {
			log.Println("Can't fetch google :(")
			return nil, err
		}

		return req, nil
	})
	log.Println("Google fetched successfully!")
}
