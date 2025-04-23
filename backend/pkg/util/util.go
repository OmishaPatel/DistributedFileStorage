package util


func GetServerAddress(serverID string) string {
	// Map server IDs to their addresses
	serverAddresses := map[string]string{
		"server1": "http://localhost:8081",
		"server2": "http://localhost:8082",
		"server3": "http://localhost:8083",
		"server4": "http://localhost:8084",
	}
	return serverAddresses[serverID]
}
