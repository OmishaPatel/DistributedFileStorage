package main

import (
	"flag"
	"log"
	"backend/pkg/server"
)

func main() {
	port := flag.String("port", "8084", "Port to run the server on")
	uploadDir := flag.String("upload-dir", "./fileStorage/server4", "Directory to store uploaded files")
	flag.Parse()
	srv, err := server.NewStorageNodeServer(server.StorageNodeConfig{
    ServerID: "server4",
    UploadDir: *uploadDir,
})

	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}
	log.Printf("Server 4 started on port %s", *port)
	if err:= srv.Run(":" + *port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}