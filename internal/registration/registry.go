package registration

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"path"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hexablock/vivaldi"
	"github.com/serverledge-faas/serverledge/internal/node"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/utils"
	"go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
)

const registryBaseDirectory = "registry"
const registryLoadBalancerDirectory = "__lb"
const anchorsBaseDirectory = "anchor"
const etcdLeaseTTL = 120

var amAnchor = false

var neighborMu sync.RWMutex
var remoteMu sync.RWMutex
var failureMu sync.RWMutex

var nearestNeighbors []NodeRegistration

var neighborInfo map[string]*StatusInformation
var neighbors map[string]NodeRegistration
var neighborFailureInfos []*FailureInfo

var remoteNodes map[string]NodeRegistration  //per nodi remoti
var remoteInfo map[string]*StatusInformation //per nodi remoti

var radius int64
var anchorVivaldi *vivaldi.Client

var remoteOffloadingTarget NodeRegistration
var remoteOffloadingTargetLatencyMs float64

var LocalVivaldiClient *vivaldi.Client  //changed name from VialdiClient
var RemoteVivaldiClient *vivaldi.Client //new vivaldi client for remote area
var SelfRegistration *NodeRegistration

var etcdClient *clientv3.Client = nil
var etcdLease clientv3.LeaseID

func (r *NodeRegistration) toEtcdKey() (key string) {
	if r.IsLoadBalancer {
		return fmt.Sprintf("%s/%s/%s/%s/%s", registryBaseDirectory, r.Area, registryLoadBalancerDirectory, r.NodeID.Arch, r.Key)
	} else {
		return fmt.Sprintf("%s/%s/%s/%s", registryBaseDirectory, r.Area, r.NodeID.Arch, r.Key)
	}
}

func (r *NodeRegistration) APIUrl() (url string) {
	return fmt.Sprintf("http://%s:%d", r.IPAddress, r.APIPort)
}

func areaEtcdKey(area string) string {
	return fmt.Sprintf("%s/%s/", registryBaseDirectory, area)
}

func anchorsEtcdKey() string {
	return fmt.Sprintf("%s/%s/", registryBaseDirectory, anchorsBaseDirectory)
}

// questa funzione è l'equivalente di JoinAreaPharos() ma per il consistent hashing
func JoinConsistentHashArea() error {
	var err error
	etcdClient, err = utils.GetEtcdClient()
	if err != nil {
		log.Fatal(UnavailableClientErr)
		return UnavailableClientErr
	}

	area, err := findAreaPharos()
	if err != nil {
		log.Fatal(err) //TODO come gestire l'errore?
	}

	//i controlli sono già fatti in findAreaPharos
	if area == "" {
		//non ho trovato area, quindi la creo e genero id randomico hashato
		tempNode := setUpNodeId("")
		node.LocalNode = tempNode

		err = createNewArea()
		if err != nil {
			return nil //TODO gestire correttamente questo errore
		}

	} else {
		//qui ho ottenuto un'area
		node.LocalNode = setUpNodeId(area)
		node.LocalNode.Area = area

	}

	//si registra su etcd
	err = RegisterNode()
	if err != nil {
		log.Fatal(err)
		return err
	}

	nodes, err := GetNodesInArea(node.LocalNode.Area, true, 0)
	if err != nil {
		return err
	}
	//creo l'hash ring con tutti i nodi dell'area
	SetUpRing(nodes)

	return nil
}

// CheckNewNode verifica se il nodo attuale conosce un nodo con id: key
func CheckNewNode(key string, addr string, port int) {
	//mutex già lockato nella funzione GetPeerFromKey
	newNode := GetPeerFromKey(key)

	//se entro, significa che tale nodo non esiste
	if newNode == nil {
		InsertNewNode(key, addr, port)
	} else {
		failureMu.Lock()
		for i := range neighborFailureInfos {
			temp := neighborFailureInfos[i]
			if temp.NodeKey == key {
				//rimetto il nodo in fondo
				copy(neighborFailureInfos[i:], neighborFailureInfos[i+1:])
				neighborFailureInfos[len(neighborFailureInfos)-1] = temp
				temp.NodeAlive()
				break
			}
		}
		failureMu.Unlock()
	}
}

// funzione che gestisce il caso in cui un nodo contattato non ha risposto
func handleNeighborTimeout(node *NodeRegistration) {
	failureMu.Lock()
	for i := range neighborFailureInfos {
		temp := neighborFailureInfos[i]
		if temp.NodeKey == node.Key {
			temp.NodeDead()
			break
		}
	}
	failureMu.Unlock()
}

// InsertNewNode aggiunge un nuovo nodo se non presente alle corrette strutture dati
func InsertNewNode(key string, addr string, port int) {

	var newNode NodeRegistration
	newNode.Area = node.LocalNode.Area
	newNode.UDPPort = 9876
	newNode.Key = key
	newNode.IPAddress = addr
	newNode.IsLoadBalancer = false

	arch, api := archAPIRequest(&newNode)

	newNode.Arch = arch
	newNode.APIPort = api

	neighborMu.Lock()
	//aggiungo nodo ai nodi vicini
	neighbors[newNode.Key] = newNode
	neighborMu.Unlock()

	temp := &FailureInfo{
		NodeKey:   key,
		DeadTimes: 0,
		LastSeen:  time.Now().UnixMilli(),
	}
	failureMu.Lock()
	neighborFailureInfos = append(neighborFailureInfos, temp)
	failureMu.Unlock()

	InsertNodeHash(newNode)

}

// funzione che rimuove un nodo data una stringa (id nodo)
func removeNeighborNode(key string) {
	neighborMu.Lock()
	if _, ok := neighbors[key]; ok {
		RemoveNode(key, neighbors[key].Arch)
		delete(neighbors, key)
		delete(neighborInfo, key)
		neighborMu.Unlock()
		return
	}
	neighborMu.Unlock()

	return
}

// questa funzione trova l'area più vicina e la joina (altrimenti la crea)
// effettua anche la registrazione
func JoinAreaPharos() error {
	var err error
	etcdClient, err = utils.GetEtcdClient()
	if err != nil {
		log.Fatal(UnavailableClientErr)
		return UnavailableClientErr
	}

	area, err := findAreaPharos()
	if err != nil {
		log.Fatal(err) //TODO come gestire l'errore?
	}

	//setto id del nodo
	node.LocalNode = setUpNodeId(area)

	//entro se devo creare una nuova area (area più vicina non è sufficientemente vicina)
	if area == "" {
		err = createNewArea()
		if err != nil {
			return nil //TODO gestire correttamente questo errore
		}
	} else {
		node.LocalNode.Area = area
	}
	return nil
}

// modo classico per creare un id
func setUpNodeId(area string) node.NodeID {

	myId := config.GetString(config.REGISTRY_NODE_ID, "")
	if myId == "" {
		return node.NewRandomIdentifier(area)
	} else {
		return node.NewIdentifier(myId, area)
	}
}

// funzione che crea una nuova area
func createNewArea() error {
	area := "Area-" + node.LocalNode.Key

	log.Println("New area registered: " + area)
	node.LocalNode.Area = area

	//aggiunta della nuova anchor, ovvero nodo corrente
	anchorDir := anchorsEtcdKey()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	//recupero indirizzo ip
	defaultAddressStr := "127.0.0.1"
	address, err := utils.GetOutboundIp()
	if err == nil {
		defaultAddressStr = address.String()
	}

	//recupero porta api, porta udp e arch
	apiPort := config.GetInt(config.API_PORT, 1323)
	udpPort := config.GetInt(config.LISTEN_UDP_PORT, 9876)

	//chiave per etcd
	anchorKey := path.Join(anchorDir, area, node.LocalNode.Key)

	//valore associato alla chiave
	anchorValue := fmt.Sprintf("%s;%d;%d;%s", defaultAddressStr, apiPort, udpPort, node.LocalNode.Arch)

	//aggiungo la nuova anchor nella lista di anchor: "AreaName/anchorName/anchorIP;APIPort;UDPPort;arch"
	_, err = etcdClient.Put(ctx, anchorKey, anchorValue)
	if err != nil {
		return fmt.Errorf("impossibile aggiungere nuova anchor: %w", err)
	}

	log.Printf("Salvataggio su etcd -> Chiave: %s | Valore: %s\n", anchorKey, anchorValue)

	//poichè io creo la nuova area, io sono la nuova anchor
	amAnchor = true
	return nil
}

// funzione che ritorna tutte le anchor registrate in Etcd
func getPharosAnchors() (map[string]NodeRegistration, error) {
	prefix := anchorsEtcdKey()
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)

	//resp conterrà tutte le chiavi "AreaName/anchorName/anchorIP;APIPort;UDPPort;arch"
	resp, err := etcdClient.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		fmt.Println(err)
		utils.TriggerEtcdReconnection()
		return nil, err
	}

	anchors := make(map[string]NodeRegistration)

	for _, kv := range resp.Kvs {
		fullKey := string(kv.Key)
		//per ora ho registry/anchor/areaName/nodeName/
		parts := strings.Split(fullKey, "/")
		if len(parts) != 4 {
			//ignore current anchor
			continue
		}
		areaName := parts[2]
		nodeKey := parts[3]
		payload := kv.Value

		//creo il nodo ancora
		newAnchor, err := parseEtcdRegisteredNode(areaName, nodeKey, payload)
		if err != nil {
			continue
		}
		anchors[nodeKey] = newAnchor
	}

	return anchors, nil
}

// questa funzione ritorna l'area più vicina e relativo rtt
func findAreaPharos() (string, error) {

	anchors, err := getPharosAnchors()
	if err != nil {
		fmt.Println("No anchors found")
		return "", err
	}

	minAreaName := ""
	minRtt := time.Duration(math.MaxInt64)

	for _, anchor := range anchors {
		_, rtt, currRad := anchorInfoRequest(&anchor)

		if rtt.Milliseconds() == 0 {
			minRtt = rtt
			minAreaName = anchor.Area
			continue
		}
		//fmt.Println("Correctly contacted anchor " + key + " with RTT: " + rtt.String())

		if currRad == 0 && rtt.Milliseconds() < config.MAX_AREA_DISTANCE {
			minRtt = rtt
			minAreaName = anchor.Area
			continue
		}

		if rtt.Milliseconds() >= 0 && rtt.Milliseconds() > 2*currRad {
			log.Println("Node too far from max radius") //todo modificare meglio questo
			continue
		}
		if rtt < minRtt {
			minRtt = rtt
			minAreaName = anchor.Area
		}
	}

	return minAreaName, err
}

// RegisterNode make a registration to the local Area
func registerToEtcd(asLoadBalancer bool) error {
	log.Printf("Registration for node: %s\n", node.LocalNode)

	defaultAddressStr := "127.0.0.1"
	address, err := utils.GetOutboundIp() //recupero indirizzo ip
	if err == nil {
		defaultAddressStr = address.String()
	}

	etcdClient, err = utils.GetEtcdClient()
	if err != nil {
		log.Fatal(UnavailableClientErr)
		return UnavailableClientErr
	}

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	resp, err := etcdClient.Grant(ctx, etcdLeaseTTL)
	if err != nil {
		log.Fatalf("Could not grant lease: %v", err)
		return err
	}
	etcdLease = resp.ID

	registeredLocalIP := config.GetString(config.API_IP, defaultAddressStr)
	apiPort := config.GetInt(config.API_PORT, 1323)
	udpPort := config.GetInt(config.LISTEN_UDP_PORT, 9876)
	arch := runtime.GOARCH

	payload := fmt.Sprintf("%s;%d;%d;%s", registeredLocalIP, apiPort, udpPort, arch)

	SelfRegistration = &NodeRegistration{NodeID: node.LocalNode, IPAddress: registeredLocalIP, APIPort: apiPort, UDPPort: udpPort, IsLoadBalancer: asLoadBalancer}

	// save couple (id, hostport) to the correct Area-dir on etcd
	etcdKey := SelfRegistration.toEtcdKey()
	log.Printf("Registering to etcd: %s\n", etcdKey)
	_, err = etcdClient.Put(ctx, etcdKey, payload, clientv3.WithLease(etcdLease))
	if err != nil {
		log.Fatal(IdRegistrationErr)
		return IdRegistrationErr
	}

	go func() {
		ticker := time.NewTicker(etcdLeaseTTL * 0.75 * time.Second)
		for {
			select {
			case <-ticker.C:
				keepAliveLease()
			}
		}
	}()

	return nil
}

// RegisterNode make a registration to the local Area
func RegisterNode() error {
	return registerToEtcd(false)
}

func RegisterLoadBalancer() error {
	return registerToEtcd(true)
}

func keepAliveLease() {
	_, err := etcdClient.KeepAliveOnce(context.Background(), etcdLease)
	if err != nil {
		log.Printf("Error keeping alive lease: %v", err)
	}
}

func parseEtcdRegisteredNode(area string, key string, payload []byte) (NodeRegistration, error) {
	payloadStr := string(payload)
	split := strings.Split(payloadStr, ";")
	if len(split) < 4 {
		return NodeRegistration{}, fmt.Errorf("invalid payload: %s", payloadStr)
	}

	ipAddress := split[0]

	apiPort, err := strconv.Atoi(split[1])
	if err != nil {
		return NodeRegistration{}, err
	}

	udpPort, err := strconv.Atoi(split[2])
	if err != nil {
		return NodeRegistration{}, err
	}

	arch := split[3]

	return NodeRegistration{NodeID: node.NodeID{Area: area, Key: key, Arch: arch}, IPAddress: ipAddress, APIPort: apiPort, UDPPort: udpPort}, nil
}

// funzione che restituisce maxRandom nodi remoti in modo randomico
func GetRandomRemoteNodes(area string, includeSelf bool, limit int64, maxRandom int) (map[string]NodeRegistration, error) {
	anchors, err := getPharosAnchors()
	if err != nil {
		log.Printf("Error parsing anchor %s: %v\n", area, err)
		return nil, err
	}

	//codice ripetuto anche in GetRandomNodesInArea
	//creo una slice in cui metto le chiavi e poi le mischio
	keys := make([]string, 0, len(anchors))
	for k := range anchors {
		if !includeSelf && area == SelfRegistration.Area && k == SelfRegistration.Key {
			continue
		}
		keys = append(keys, k)
	}

	//qui mischio le chiavi
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	//creo la map finale
	randomServers := make(map[string]NodeRegistration, maxRandom)

	//dallo slice mischiato prendo solo i primi maxRandom
	if maxRandom > len(keys) {
		maxRandom = len(keys)
	}
	for i := 0; i < maxRandom; i++ {
		randomKey := keys[i]
		randomServers[randomKey] = anchors[randomKey]
	}

	return randomServers, nil
}

// come GetRandomNodesInArea() ma ritorna maxRandom elementi vicini presi randomicamente
func GetRandomNodesInArea(area string, includeSelf bool, limit int64, maxRandom int) (map[string]NodeRegistration, error) {
	baseDir := areaEtcdKey(area)
	lbPrefix := path.Join(baseDir, registryLoadBalancerDirectory)

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)

	if limit < 0 {
		limit = 0 // no limit
	}

	resp, err := etcdClient.Get(ctx, baseDir, clientv3.WithPrefix(), clientv3.WithLimit(limit))
	if err != nil {
		utils.TriggerEtcdReconnection()
		return nil, fmt.Errorf("Could not read from etcd: %v", err)
	}

	//come originale ma salvo tutti i server
	allServers := make(map[string]NodeRegistration)
	for _, s := range resp.Kvs {
		if strings.HasPrefix(string(s.Key), lbPrefix) {
			// skip LB
			continue
		}
		key := path.Base(string(s.Key))
		if !includeSelf && area == SelfRegistration.Area && key == SelfRegistration.Key {
			continue
		}

		reg, err := parseEtcdRegisteredNode(area, key, s.Value)
		if err == nil {
			allServers[key] = reg
		}
	}

	//qui la grossa modifica
	//ritorno tutti anche se maxRandom <= 0
	if maxRandom <= 0 || len(allServers) <= maxRandom {
		return allServers, nil
	}

	//creo una slice in cui metto le chiavi e poi le mischio
	keys := make([]string, 0, len(allServers))
	for k := range allServers {
		keys = append(keys, k)
	}

	//qui mischio le chiavi
	rand.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	//creo la map finale
	randomServers := make(map[string]NodeRegistration, maxRandom)
	//dallo slice mischiato prendo solo i primi maxRandom
	for i := 0; i < maxRandom; i++ {
		randomKey := keys[i]
		randomServers[randomKey] = allServers[randomKey]
	}

	return randomServers, nil
}

// ritorna max nodi vicini o remoti in modo randomico dalla propria lista di nodi conosciuti
func getRandomNodes(isRemote bool, max int) []NodeRegistration {
	nodes := make([]NodeRegistration, 0)
	var n int
	if isRemote {
		//ritorno da remoteNodes
		remoteMu.RLock()
		for _, reg := range remoteNodes {
			nodes = append(nodes, reg)
		}
		//ritorno tutti se sono pochi
		n = len(remoteNodes)
		remoteMu.RUnlock()
		if n <= max {
			return nodes
		}
	} else {
		//ritorno da localNodes
		neighborMu.RLock()
		for _, reg := range neighbors {
			nodes = append(nodes, reg)
		}
		//ritorno tutti se sono pochi
		n = len(neighbors)
		neighborMu.RUnlock()
		if n <= max {
			return nodes
		}
	}

	//array di indici che poi mischio
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}

	//mischio gli indici
	rand.Shuffle(n, func(i, j int) {
		indices[i], indices[j] = indices[j], indices[i]
	})

	nodesToUpdate := make([]NodeRegistration, max)

	//prendo max elementi di nodes usando i primi max elementi dell'array di indici
	for i := 0; i < max; i++ {
		nodesToUpdate[i] = nodes[indices[i]]
	}

	return nodesToUpdate
}

// GetNodesInArea is used to obtain the list of  other server's addresses under a specific local Area
func GetNodesInArea(area string, includeSelf bool, limit int64) (map[string]NodeRegistration, error) {
	baseDir := areaEtcdKey(area)
	lbPrefix := path.Join(baseDir, registryLoadBalancerDirectory)

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)

	if limit < 0 {
		limit = 0 // no limit
	}
	resp, err := etcdClient.Get(ctx, baseDir, clientv3.WithPrefix(), clientv3.WithLimit(limit))
	if err != nil {
		utils.TriggerEtcdReconnection()
		return nil, fmt.Errorf("Could not read from etcd: %v", err)
	}

	servers := make(map[string]NodeRegistration)
	for _, s := range resp.Kvs {
		if strings.HasPrefix(string(s.Key), lbPrefix) {
			// skip LB
			continue
		}
		key := path.Base(string(s.Key))
		if !includeSelf && area == SelfRegistration.Area && key == SelfRegistration.Key {
			continue
		}

		reg, err := parseEtcdRegisteredNode(area, key, s.Value) //ritorna il nodo con le info di network
		if err == nil {
			servers[key] = reg
			//fmt.Printf("Server found: %v (%v-udp:%d)\n", servers[key], reg.IPAddress, reg.UDPPort)
		}
	}

	return servers, nil
}

func GetOneNodeInArea(area string, includeSelf bool) (NodeRegistration, error) {
	nodes, err := GetNodesInArea(area, includeSelf, 1)
	if err == nil {
		for _, n := range nodes {
			return n, nil
		}
		return NodeRegistration{}, fmt.Errorf("no nodes found")
	}

	return NodeRegistration{}, err
}

func GetLBInArea(area string) (map[string]NodeRegistration, error) {
	prefix := areaEtcdKey(area)
	baseDir := path.Join(prefix, registryLoadBalancerDirectory)

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)

	resp, err := etcdClient.Get(ctx, baseDir, clientv3.WithPrefix())
	if err != nil {
		utils.TriggerEtcdReconnection()
		return nil, fmt.Errorf("Could not read from etcd: %v", err)
	}

	servers := make(map[string]NodeRegistration)

	for _, s := range resp.Kvs {
		key := path.Base(string(s.Key))
		reg, err := parseEtcdRegisteredNode(area, key, s.Value)
		if err == nil {
			reg.IsLoadBalancer = true
			servers[key] = reg
			fmt.Printf("Server found: %v\n", servers[key])
		}
	}

	return servers, nil
}

func Deregister() error {
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
	_, err := etcdClient.Revoke(ctx, etcdLease)
	if err != nil {
		log.Printf("Error revoking lease: %v", err)
	}

	return nil
}

func StartMonitoring() error {

	neighbors = make(map[string]NodeRegistration)      //network info
	neighborInfo = make(map[string]*StatusInformation) //scheduling info

	remoteNodes = make(map[string]NodeRegistration) //per coordinate remote
	remoteInfo = make(map[string]*StatusInformation)

	defaultConfig := vivaldi.DefaultConfig()
	defaultConfig.Dimensionality = 3
	var err error
	LocalVivaldiClient, err = vivaldi.NewClient(defaultConfig)
	if err != nil {
		return err
	}

	//creo il nuovo client per coordinate remote
	RemoteVivaldiClient, err = vivaldi.NewClient(defaultConfig) //TODO modificare con i parametri migliori
	if err != nil {
		return err
	}

	//init dei neighbor
	// gets info from Etcd about all the other nodes in the same area
	newNearNeighbors, err := GetNodesInArea(SelfRegistration.Area, false, 0)
	if err != nil {
		log.Println(err)
		return err
	}

	//aggiungo tutti i nodi registrati e le info di failure
	for key := range newNearNeighbors {
		neighbors[key] = newNearNeighbors[key]

		temp := FailureInfo{
			NodeKey:   key,
			DeadTimes: 0,
			LastSeen:  time.Now().UnixMilli(),
		}
		neighborFailureInfos = append(neighborFailureInfos, &temp)
	}

	//complete globalMonitoring phase at startup
	globalMonitoring()

	// start listening for incoming udp connections; use case: edge-nodes request for status infos
	go UDPStatusServer()
	go runMonitor()
	go monitorFailure()

	if amAnchor {
		log.Printf("Starting Pharos Anchor Monitoring\n")
		anchorVivaldi, err = vivaldi.NewClient(defaultConfig)
		if err != nil {
			return err
		}
		go monitorArea()
	}
	return nil
}

func monitorFailure() {
	checkTimer := time.NewTicker(time.Duration(config.GetInt(config.REG_NEARBY_INTERVAL, 2)*3) * time.Second)
	for {
		select {
		case <-checkTimer.C:
			deadCollector()
		}
	}
}

// controlla se ci sono nodi spenti e li elimina di conseguenza
func deadCollector() {
	fmt.Println("Dead Collector in action")
	failureMu.Lock()
	for i := range neighborFailureInfos {
		curr := neighborFailureInfos[i]
		if (time.Now().UnixMilli()-curr.LastSeen) > 2000 || curr.DeadTimes > 3 {
			//nodo da rimuovere
			log.Println("Removed offline node: " + curr.NodeKey)
			removeNeighborNode(curr.NodeKey)
			neighborFailureInfos = slices.Delete(neighborFailureInfos, i, i+1)
		}
	}
	failureMu.Unlock()
}

func runMonitor() {
	nearbyTicker := time.NewTicker(time.Duration(config.GetInt(config.REG_NEARBY_INTERVAL, 2)) * time.Second)         //wake-up nearby globalMonitoring
	monitoringTicker := time.NewTicker(time.Duration(config.GetInt(config.REG_MONITORING_INTERVAL, 2)) * time.Second) // wake-up general-area globalMonitoring
	for {
		select {
		case <-monitoringTicker.C:
			globalMonitoring()
		case <-nearbyTicker.C:
			nearbyMonitoring(LocalVivaldiClient)
		}
	}
}

func monitorArea() {
	radiusTicker := time.NewTicker(time.Duration(config.GetInt(config.REG_NEARBY_INTERVAL, 20)) * time.Second)
	centroidTicker := time.NewTicker(time.Duration(config.GetInt(config.REG_NEARBY_INTERVAL, 20)) * time.Second)
	for {
		select {
		case <-radiusTicker.C:
			calculateRadius()
		case <-centroidTicker.C:
			calculateCentroid()
		}
	}
}

// calculateRadius calcola il raggio massimo dell'area attuale. eseguito solo dall'anchor
func calculateRadius() {
	neighborMu.RLock()
	var maxDistance int64
	for _, n := range neighborInfo {
		temp := anchorVivaldi.DistanceTo(&n.Coordinates).Milliseconds()
		if temp > maxDistance {
			maxDistance = temp
		}
	}
	log.Println("Current Radius:", maxDistance)
	radius = maxDistance
	neighborMu.RUnlock()
}

// calculateCentroid calcola il centro della zona di nodi come media aritmetica delle componenti dei nodi.
// eseguito solo dall'anchor
func calculateCentroid() {
	var sumX, sumY, sumZ float64
	var centroid = vivaldi.Coordinate{}
	neighborMu.RLock()

	for _, n := range neighborInfo {
		temp := n.Coordinates.Vec
		sumX += temp[0]
		sumY += temp[1]
		sumZ += temp[2]
	}

	neighborMu.RUnlock()

	//aggiungo coordinate anchor
	sumX += LocalVivaldiClient.GetCoordinate().Vec[0]
	sumY += LocalVivaldiClient.GetCoordinate().Vec[1]
	sumZ += LocalVivaldiClient.GetCoordinate().Vec[2]

	sumX /= float64(len(neighborInfo) + 1)
	sumY /= float64(len(neighborInfo) + 1)
	sumZ /= float64(len(neighborInfo) + 1)

	centroid.Vec = []float64{sumX, sumY, sumZ}
	centroid.Error = 0.0
	centroid.Height = 0.0
	centroid.Adjustment = 0.0
	err := anchorVivaldi.SetCoordinate(&centroid)
	if err != nil {
		log.Printf("Error setting coordinate centroide: %v", err)
	}

}

func globalMonitoring() {

	// gets info from Etcd about all the other areas
	newRemoteNodes, err := GetRandomRemoteNodes(SelfRegistration.Area, false, 0, 0) //config.MAX_VIVALDI_NEAR_NODES)
	if err != nil {
		log.Println(err)
		return
	}

	remoteMu.Lock()
	//elimino nodi remoti non più registrati
	for key := range remoteInfo {
		_, ok := newRemoteNodes[key]
		if !ok {
			delete(remoteInfo, key)
		}
	}

	//aggiungo nuovi nodi remoti
	for key := range newRemoteNodes {
		_, ok := remoteNodes[key]
		if !ok {
			remoteNodes[key] = newRemoteNodes[key]
		}
	}
	remoteMu.Unlock()

	remoteMonitoring(RemoteVivaldiClient)
	updateRemoteOffloadingTarget()    //ottiene un nodo per offloading
	updateLatencyToOffloadingTarget() //aggiorna la latenza con tale nodo
}

func updateLatencyToOffloadingTarget() {
	if remoteOffloadingTarget.Key == "" {
		remoteOffloadingTargetLatencyMs = 9999.0
		return
	}

	hostAndPort := fmt.Sprintf("%s:%d", remoteOffloadingTarget.IPAddress, remoteOffloadingTarget.APIPort)
	latency, err := tcpLatency(hostAndPort)
	if err != nil {
		log.Println(err)
	} else {
		log.Printf("Latency for remote offloading target is %v (ms)", latency)
		remoteOffloadingTargetLatencyMs = max(0.1, latency)
	}
}

func tcpLatency(hostAndPort string) (float64, error) {
	start := time.Now()
	conn, err := net.DialTimeout("tcp", hostAndPort, 3*time.Second)
	if err != nil {
		return 0, err
	}
	_ = conn.Close()
	return float64(time.Since(start).Milliseconds()), nil
}

// aggiorna il nodo di offloading. prima verifica se l'area cloud ha un lb e in caso sceglie quello,
// altrimenti cerca un nodo nella propria area.
func updateRemoteOffloadingTarget() {
	// If there is a LB in the remote area, it is used.
	// Otherwise, a random node in the area is chosen.

	remoteArea := config.GetString(config.REGISTRY_REMOTE_AREA, "")
	//cambiato il comportamento in assenza di configurazione statica
	if remoteArea == "" {
		//log.Printf("No remote area is configured; vertical offloading disabled")		OLD
		//remoteOffloadingTarget = NodeRegistration{}									OLD
		//return																		OLD

		//recupero nodo remoto più vicino dalla lista di nodi remoti
		cloud := getNearestCloud()
		if cloud == nil {
			return
		}
		remoteArea = cloud.Area
	}

	lbs, err := GetLBInArea(remoteArea)
	if err != nil {
		log.Println(err)
	}
	if err == nil && len(lbs) > 0 {
		for _, lb := range lbs {
			log.Printf("Using LB as offloading target: %v", lb.NodeID)
			remoteOffloadingTarget = lb
			return
		}
	}

	remoteNode, err := GetOneNodeInArea(remoteArea, false)
	if err == nil {
		log.Printf("Using as offloading target: %v", remoteNode.NodeID)
		remoteOffloadingTarget = remoteNode
	}
}

// ritorna l'area esterna più vicina
func getNearestCloud() *NodeRegistration {
	nearestKey := ""
	minDistance := time.Duration(math.MaxInt64) //valore massimo come riferimento

	for key, info := range remoteInfo {

		//ottengo distanza attuale
		distance := RemoteVivaldiClient.DistanceTo(&info.Coordinates)

		if distance < minDistance {
			minDistance = distance
			nearestKey = key
		}
	}

	if nearestKey == "" {
		return nil
	}

	//ottengo il nodo più vicino
	nearestNode, ok := remoteNodes[nearestKey]
	if !ok {
		return nil
	}
	return &nearestNode
}

// computeNearestNeighbors finds servers nearby to the current one
func computeNearestNeighbors(nNeighbors int) {
	type dist struct {
		key      string
		distance time.Duration
	}

	//all neighbors are considered the nearest
	if nNeighbors > len(neighborInfo) {
		nearestNeighbors = make([]NodeRegistration, 0, len(neighborInfo))
		for _, n := range neighbors {
			nearestNeighbors = append(nearestNeighbors, n)
		}
		return
	}

	var distanceBuf = make([]dist, len(neighborInfo)) //distances from current server
	for key, s := range neighborInfo {
		distanceBuf = append(distanceBuf, dist{key, LocalVivaldiClient.DistanceTo(&s.Coordinates)})
	}
	sort.Slice(distanceBuf, func(i, j int) bool { return distanceBuf[i].distance < distanceBuf[j].distance })

	nearestNeighbors = make([]NodeRegistration, nNeighbors)
	for i := 0; i < nNeighbors; i++ {
		k := distanceBuf[i].key
		nearestNeighbors[i] = neighbors[k]
	}
}

// remoteMonitoring check remote node's status
func remoteMonitoring(vivaldiClient *vivaldi.Client) {
	//log.Printf("Periodic remote Monitoring\n")
	peersToUpdate := getRandomNodes(true, config.MAX_VIVALDI_NEAR_NODES)
	if len(peersToUpdate) == 0 {
		return
	}
	for _, registeredNode := range peersToUpdate {
		coords, rtt, _ := anchorInfoRequest(&registeredNode) //recupero RTT e coordinate in status info

		if coords == nil {
			log.Printf("Unreachable remote node: %s\n", registeredNode.NodeID)
			continue
		}
		remoteMu.Lock()
		_, ok := remoteInfo[registeredNode.Key]
		if ok {
			remoteInfo[registeredNode.Key].Coordinates = *coords
			remoteInfo[registeredNode.Key].LastUpdateTime = time.Now().Unix()
		} else {
			newInfo := StatusInformation{
				Coordinates:    *coords,
				LastUpdateTime: time.Now().Unix()}
			remoteInfo[registeredNode.Key] = &newInfo
		}

		_, err := vivaldiClient.Update(registeredNode.NodeID.Key, coords, rtt) //NEW
		if err != nil {
			log.Printf("Error while updating node coordinates: %s\n", err)
		}
		remoteMu.Unlock()
	}
}

// nearbyMonitoring check nearby server's status
func nearbyMonitoring(vivaldiClient *vivaldi.Client) {
	//log.Printf("Periodic nearby Monitoring\n")

	/*mutex.RLock()		OLD
	// TODO: randomly choose a subset of peers for update?
	peersToUpdate := make([]NodeRegistration, 0)
	for _, reg := range neighbors {
		peersToUpdate = append(peersToUpdate, reg)
	}
	mutex.RUnlock()*/

	peersToUpdate := getRandomNodes(false, config.MAX_VIVALDI_NEAR_NODES) //risposta al todo
	for _, registeredNode := range peersToUpdate {
		newInfo, rtt := statusInfoRequest(&registeredNode) //recupero RTT e coordinate in status info

		if newInfo == nil {
			//log.Printf("Unreachable neighbor: %s\n", registeredNode.NodeID)
			continue
		}
		neighborMu.Lock()

		neighborInfo[registeredNode.Key] = newInfo
		neighborInfo[registeredNode.Key].LastUpdateTime = time.Now().Unix()

		//_, err := vivaldiClient.Update("node", &newInfo.Coordinates, rtt)		OLD
		_, err := vivaldiClient.Update(registeredNode.NodeID.Key, &newInfo.Coordinates, rtt) //NEW
		if err != nil {
			log.Printf("Error while updating node coordinates: %s\n", err)
		}
		neighborMu.Unlock()
	}

	// Updates neighborInfo with the N closest nodes from serverMap
	computeNearestNeighbors(2) //todo change this value, maybe tutti i nodi devono essere considerati (nodi stessa area)
	fmt.Printf("TEST: X: %f, Y: %f, Z: %f\n", LocalVivaldiClient.GetCoordinate().Vec[0],
		LocalVivaldiClient.GetCoordinate().Vec[1], LocalVivaldiClient.GetCoordinate().Vec[2])
}

func GetNearestNeighbors() []NodeRegistration {
	return nearestNeighbors
}

func GetPeerFromKey(key string) *NodeRegistration {
	//se il nodo richiesto sono io
	if key == node.LocalNode.Key {
		return SelfRegistration
	}
	neighborMu.RLock()
	defer neighborMu.RUnlock()
	reg, ok := neighbors[key]
	if !ok {
		return nil
	}
	return &reg
}

func GetRemoteFromKey(key string) *NodeRegistration {
	remoteMu.RLock()
	defer remoteMu.RUnlock()
	reg, ok := remoteNodes[key]
	if !ok {
		return nil
	}
	return &reg
}

func GetStatusInfoFromKey(key string) *StatusInformation {
	neighborMu.RLock()
	defer neighborMu.RUnlock()
	reg, ok := neighborInfo[key]
	if !ok {
		return nil
	}
	return reg
}

func GetRemoteOffloadingTarget() *NodeRegistration {
	if remoteOffloadingTarget.Key != "" {
		return &remoteOffloadingTarget
	}
	return nil
}

func GetRemoteOffloadingTargetLatencyMs() float64 {
	return remoteOffloadingTargetLatencyMs
}

func GetFullNeighborInfo() map[string]*StatusInformation {
	neighborMu.RLock()
	defer neighborMu.RUnlock()
	return maps.Clone(neighborInfo)
}
