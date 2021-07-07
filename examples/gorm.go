package main

import (
	"fmt"
	"net/http"

	nrcontext "github.com/getndazn/newrelic-context"
	"github.com/getndazn/newrelic-context/nrgorm"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var db *gorm.DB

func initDB() *gorm.DB {
	db, err := gorm.Open(sqlite.Open("./foo.db"))
	if err != nil {
		panic("failed to connect database")
	}
	nrgorm.AddGormCallbacks(db)
	return db
}

type Product struct {
	ID   int
	Name string
}

func catalogPage(db *gorm.DB) http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		var products []Product
		db := nrcontext.SetTxnToGorm(req.Context(), db)
		db.Find(&products)
		for i, v := range products {
			rw.Write([]byte(fmt.Sprintf("%v. %v\n", i, v.Name)))
		}
	})
}

func other_main() {
	db = initDB()
	handler := catalogPage(db)
	nrmiddleware, _ := nrcontext.NewMiddleware("test-app", "my-license-key")
	handler = nrmiddleware.Handler(handler)

	http.ListenAndServe(":8000", handler)
}
