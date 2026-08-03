package registration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/hexablock/vivaldi"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/node"
)

// UDPStatusServer listen for incoming request from other edge-nodes which want to retrieve the status of this server
// this listener should be called asynchronously in the main function
func UDPStatusServer() {
	port := config.GetInt(config.LISTEN_UDP_PORT, 9876)
	address := fmt.Sprintf("%s:%d", "0.0.0.0", port)
	udpAddr, err := net.ResolveUDPAddr("udp", address)

	if err != nil {
		log.Fatal(err)
	}
	// setup listener for incoming UDP connection
	udpConn, err := net.ListenUDP("udp", udpAddr)

	if err != nil {
		log.Fatal(err)
	}
	log.Printf("UDP server up and listening on port %d\n", port)

	defer func(udpConn *net.UDPConn) {
		err := udpConn.Close()
		if err != nil {
			log.Printf("Error while closing UDP connection: %s\n", err)
		}
	}(udpConn)

	for {
		// wait for UDP client to connect
		handleUDPConnection(udpConn)
	}

}

func handleUDPConnection(conn *net.UDPConn) {
	buffer := make([]byte, 1024)

	n, addr, err := conn.ReadFromUDP(buffer)
	if err != nil {
		return
	}

	message := bytes.TrimSpace(bytes.Trim(buffer[:n], "\x00"))

	var req GeneralRequest
	err = json.Unmarshal(message, &req)
	if err != nil {
		log.Println(err)
		return
	}

	//caso di comunicazione anchor-peer
	if req.Type == '1' && amAnchor {
		anchorResp, err := getAnchorResponse()
		if err != nil {
			log.Println(err)
			anchorResp = []byte("")
		}
		_, err = conn.WriteToUDP(anchorResp, addr)
		if err != nil {
			log.Println(err)
		}

	} else if req.Type == '0' {
		//caso originale
		CheckNewNode(req.NodeId, addr.IP.String(), addr.Port)
		//retrieve the current status
		msgStatus, err := getCurrentStatusInformation()
		if err != nil {
			log.Println(err)
			msgStatus = []byte("")
		}
		//send the infos back to the client edge-node
		_, err = conn.WriteToUDP(msgStatus, addr)
		if err != nil {
			log.Println(err)
		}
	} else if req.Type == '2' {
		//devo rispondere con arch e apiport
		archResp, err := getArchAPIResponse()
		if err != nil {
			log.Println(err)
			archResp = []byte("")
		}

		_, err = conn.WriteToUDP(archResp, addr)
		if err != nil {
			log.Println(err)
		}
	}
}

func getAnchorResponse() ([]byte, error) {
	response := AnchorResponse{
		Type:        '1',
		Coordinates: *LocalVivaldiClient.GetCoordinate(),
		Radius:      radius,
	}

	return json.Marshal(response)
}

// messaggio per ottenere info anchor
func getAnchorRequestMessage() ([]byte, error) {
	request := GeneralRequest{
		Type:   '1',
		NodeId: node.LocalNode.Key,
	}
	return json.Marshal(request)
}

// messaggio per ottenere status information
func getStatusRequestMessage() ([]byte, error) {
	request := GeneralRequest{
		Type:   '0',
		NodeId: node.LocalNode.Key,
	}

	return json.Marshal(request)
}

// messaggio per ottenere architettura e apiport remota
func getArchRequestMessage() ([]byte, error) {
	request := GeneralRequest{
		Type:   '2',
		NodeId: "",
	}
	return json.Marshal(request)
}

// TODO: this function should reuse the code in api.go for the /status API
func getCurrentStatusInformation() (status []byte, err error) {
	response := StatusInformation{
		AvailableWarmContainers: node.WarmStatus(),
		TotalMemory:             node.LocalResources.TotalMemory(),
		TotalCPU:                node.LocalResources.TotalCPUs(),
		AvailableMemory:         node.LocalResources.AvailableMemory(),
		FreeMemory:              node.LocalResources.FreeMemory(),
		UsedCPU:                 node.LocalResources.UsedCPUs(),
		Coordinates:             *LocalVivaldiClient.GetCoordinate(),
	}

	return json.Marshal(response)

}

func getArchAPIResponse() (status []byte, err error) {
	response := ArchAPIResponse{
		Arch:    node.LocalNode.Arch,
		APIPort: config.GetInt(config.API_PORT, 1323),
	}
	return json.Marshal(response)
}

// funzione che richiede ad una anchor informazioni
func anchorInfoRequest(anchor *NodeRegistration) (coords *vivaldi.Coordinate, rttMeasured time.Duration, radius int64) {
	hostname := anchor.IPAddress
	port := anchor.UDPPort
	address := fmt.Sprintf("%s:%d", hostname, port)
	log.Printf("Requesting anchor information for %s\n", address)

	remoteAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		log.Printf("Unreachable server %s\n", address)
		return nil, 0, 0
	}

	udpConn, err := net.DialUDP("udp", nil, remoteAddr)
	if err != nil {
		log.Println(err)
		return nil, 0, 0
	}
	defer func(udpConn *net.UDPConn) {
		err := udpConn.Close()
		if err != nil {
			log.Printf("Error while closing UDP connection: %s\n", err)
		}
	}(udpConn)

	//ottengo il messaggio corretto
	message, err := getAnchorRequestMessage()
	if err != nil {
		log.Println(err)
		return nil, 0, 0
	}
	sendingTime := time.Now()
	_, err = udpConn.Write(message)
	if err != nil {
		log.Println(err)
		return nil, 0, 0
	}

	// receive message from server
	buffer := make([]byte, 1024)
	_, _, err = udpConn.ReadFromUDP(buffer)
	if err != nil {
		log.Println(err)
		return nil, 0, 0
	}

	rtt := time.Now().Sub(sendingTime)
	buffer = bytes.Trim(buffer, "\x00")
	//unmarshal result
	var response AnchorResponse

	err = json.Unmarshal(buffer, &response)
	if err != nil {
		log.Printf("Errore durante l'unmarshal del JSON dell'Anchor: %v\n", err)
		return nil, 0, 0
	}
	log.Println("Requesting status information COMPLETED")
	return &response.Coordinates, rtt, response.Radius
}

func archAPIRequest(peer *NodeRegistration) (arch string, APIPort int) {
	hostname := peer.IPAddress
	port := peer.UDPPort
	address := fmt.Sprintf("%s:%d", hostname, port)
	log.Printf("Requesting arch-API information for %s\n", address)

	remoteAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		log.Printf("Unreachable server %s\n", address)
		return "", 0
	}

	udpConn, err := net.DialUDP("udp", nil, remoteAddr)
	if err != nil {
		log.Println(err)
		return "", 0
	}
	defer func(udpConn *net.UDPConn) {
		err := udpConn.Close()
		if err != nil {
			log.Printf("Error while closing UDP connection: %s\n", err)
		}
	}(udpConn)

	//ottengo messaggio corretto
	message, err := getArchRequestMessage()
	if err != nil {
		log.Println(err)
		return "", 0
	}
	_, err = udpConn.Write(message)
	if err != nil {
		log.Println(err)
		return "", 0
	}

	// receive message from server
	buffer := make([]byte, 1024)
	_, _, err = udpConn.ReadFromUDP(buffer)
	if err != nil {
		log.Println(err)
		return "", 0
	}

	buffer = bytes.Trim(buffer, "\x00")
	//unmarshal result
	var result ArchAPIResponse
	err = json.Unmarshal(buffer, &result)
	if err != nil {
		fmt.Println("Can not unmarshal JSON")
		return "", 0
	}

	log.Printf("Requesting arch information COMPLETED\n")

	return result.Arch, result.APIPort
}

func statusInfoRequest(peer *NodeRegistration) (info *StatusInformation, duration time.Duration) {

	hostname := peer.IPAddress
	port := peer.UDPPort
	address := fmt.Sprintf("%s:%d", hostname, port)
	log.Printf("Requesting status information for %s\n", address)

	remoteAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		log.Printf("Unreachable server %s\n", address)
		return nil, 0
	}

	udpConn, err := net.DialUDP("udp", nil, remoteAddr)
	if err != nil {
		log.Println(err)
		return nil, 0
	}
	defer func(udpConn *net.UDPConn) {
		err := udpConn.Close()
		if err != nil {
			log.Printf("Error while closing UDP connection: %s\n", err)
		}
	}(udpConn)

	// write a message to server, here 1 byte is enough
	message, err := getStatusRequestMessage() //changed, now i send my id
	if err != nil {
		log.Println(err)
		return nil, 0
	}
	sendingTime := time.Now()
	_, err = udpConn.Write(message)
	if err != nil {
		log.Println(err)
		return nil, 0
	}
	//modifica per failure detection
	timeout := (config.MAX_AREA_DISTANCE * 3) * time.Millisecond
	err = udpConn.SetReadDeadline(time.Now().Add(timeout))
	if err != nil {
		log.Println("Impossibile impostare la deadline:", err)
		return nil, 0
	}
	// receive message from server
	buffer := make([]byte, 1024)
	_, _, err = udpConn.ReadFromUDP(buffer)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			//scaduto il timer
			log.Println("Connection timed out with node: " + peer.Key)
			handleNeighborTimeout(peer)
			return nil, 0
		}
		log.Println(err)
		return nil, 0
	}

	rtt := time.Now().Sub(sendingTime)
	buffer = bytes.Trim(buffer, "\x00")
	//unmarshal result
	var result StatusInformation
	err = json.Unmarshal(buffer, &result)
	if err != nil {
		fmt.Println("Can not unmarshal JSON")
		return nil, 0
	}

	log.Printf("Requesting status information COMPLETED\n")

	return &result, rtt
}
