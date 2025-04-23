package main

import (
	"flag"
	"log"
	"backend/pkg/server"
)

func main() {
	port := flag.String("port", "8081", "Port to run the server on")
	uploadDir := flag.String("upload-dir", "./fileStorage/server1", "Directory to store uploaded files")
	flag.Parse()
	srv, err := server.NewStorageNodeServer(server.StorageNodeConfig{
    ServerID: "server1",
    UploadDir: *uploadDir,
})

	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	log.Printf("Server 1 started on port %s", *port)
	if err:= srv.Run(":" + *port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}