package main

import (
	"distributed_services_with_go/internal/server"
	"log"
)

func main() {
	src := server.NewHttpServer(":8080")
	log.Fatal(src.ListenAndServe())
}
