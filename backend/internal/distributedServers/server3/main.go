package main

import (
	"flag"
	"log"
	"backend/pkg/server"
)

func main() {
	port := flag.String("port", "8083", "Port to run the server on")
	uploadDir := flag.String("upload-dir", "./fileStorage/server3", "Directory to store uploaded files")
	flag.Parse()
	srv, err := server.NewStorageNodeServer(server.StorageNodeConfig{
    ServerID: "server3",
    UploadDir: *uploadDir,
})

	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	log.Printf("Server 3 started on port %s", *port)
	if err:= srv.Run(":" + *port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}