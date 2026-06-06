package registration

import (
	"fmt"
	"log"
	"math"
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

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/utils"
	"go.etcd.io/etcd/client/v3"
	"golang.org/x/net/context"
)

const registryBaseDirectory = "registry"
const registryLoadBalancerDirectory = "__lb"
const anchorsBaseDirectory = "anchor"
const etcdLeaseTTL = 120

var mutex sync.RWMutex

var nearestNeighbors []NodeRegistration
var neighborInfo map[string]*StatusInformation
var neighbors map[string]NodeRegistration

var remoteOffloadingTarget NodeRegistration
var remoteOffloadingTargetLatencyMs float64

var VivaldiClient *vivaldi.Client
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

// funzione che crea una nuova area edge e la aggiunge al registry
func registerNewArea(nodeId string) (string, error) {
	tempArea := "Area-" + nodeId
	//aggiungo nuova area al registry
	baseDir := areaEtcdKey(tempArea)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	//creo area con un valore useless
	_, err := etcdClient.Put(ctx, baseDir, "initialized")
	if err != nil {
		return "", fmt.Errorf("impossibile craere la regione: %w", err)
	}

	return tempArea, nil
}

func JoinAreaPharos() error {
	area, rtt, err := findAreaPharos()
	if err != nil {
		log.Fatal(err) //TODO come gestire l'errore?
	}

	if rtt.Milliseconds() >= config.MAX_AREA_DISTANCE {
		area = "" //necessito di creare una nuova regione, per ora la lascio vuota e creo NodeId
	}

	myId := config.GetString(config.REGISTRY_NODE_ID, "")
	if myId == "" {
		node.LocalNode = node.NewRandomIdentifier(area)
	} else {
		node.LocalNode = node.NewIdentifier(myId, area)
	}
	log.Printf("Local node id: %s (arch: %v)", node.LocalNode.String(), node.LocalNode.Arch)

	//entro se devo creare una nuova area
	if area == "" {
		area, err = registerNewArea(node.LocalNode.Key)
		if err != nil {
			log.Fatal(err) //TODO come gestire l'errore?
		}
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
		//recupero porta udp
		port := config.GetInt(config.LISTEN_UDP_PORT, 9876)

		temp := fmt.Sprintf("%s/%s:%d", area, defaultAddressStr, port)

		//aggiungo la nuova anchor nella lista di anchor: AreaName/IPAnchor:Port
		_, err = etcdClient.Put(ctx, anchorDir, temp)
		if err != nil {
			return fmt.Errorf("impossibile aggiungere nuova anchor: %w", err)
		}
	}
	return nil
}

func findAreaPharos() (string, time.Duration, error) {
	prefix := anchorsEtcdKey()

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)

	//resp conterrà tutte le chiavi "AreaName/anchorIP:anchorPort"
	resp, err := etcdClient.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		fmt.Println(err)
		utils.TriggerEtcdReconnection()
		return "", 0, err
	}

	anchors := make([]NodeRegistration, 0)

	for _, kv := range resp.Kvs {
		fullKey := string(kv.Key)
		parts := strings.Split(fullKey, "/")
		if len(parts) != 2 {
			//ignore current anchor
			continue
		}
		areaName := parts[0]
		ipPort := parts[1]
		ip, portStr, err := net.SplitHostPort(ipPort)
		if err != nil {
			fmt.Printf("Error parsing anchor %s: %v\n", areaName, err)
		}

		port, err := strconv.Atoi(portStr)
		if err != nil {
			fmt.Println("Errore: la stringa non è un numero valido!", err)
			return "", 0, err
		}

		newAnchor := NodeRegistration{
			NodeID: node.NodeID{
				Area: areaName,
			},
			IPAddress:      ip,
			APIPort:        0,
			UDPPort:        port,
			IsLoadBalancer: false, //TODO ogni anchor è un load balancer o possono esserlo?
		}

		anchors = append(anchors, newAnchor)
	}

	minAreaName := ""
	minRtt := time.Duration(math.MaxInt64)

	for _, anchor := range anchors {
		newInfo, rtt := statusInfoRequest(&anchor)

		if newInfo == nil {
			log.Printf("Unreachable neighbor: %s\n", anchor.Area)
			continue
		}
		if rtt < minRtt {
			minRtt = rtt
			minAreaName = anchor.Area
		}
	}

	return minAreaName, minRtt, err
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

	defaultConfig := vivaldi.DefaultConfig()
	defaultConfig.Dimensionality = 3
	var err error
	VivaldiClient, err = vivaldi.NewClient(defaultConfig)
	if err != nil {
		return err
	}

	//complete globalMonitoring phase at startup
	globalMonitoring()

	// start listening for incoming udp connections; use case: edge-nodes request for status infos
	go UDPStatusServer()
	go runMonitor()

	return nil
}

func runMonitor() {
	nearbyTicker := time.NewTicker(time.Duration(config.GetInt(config.REG_NEARBY_INTERVAL, 20)) * time.Second)         //wake-up nearby globalMonitoring
	monitoringTicker := time.NewTicker(time.Duration(config.GetInt(config.REG_MONITORING_INTERVAL, 30)) * time.Second) // wake-up general-area globalMonitoring
	for {
		select {
		case <-monitoringTicker.C:
			globalMonitoring()
		case <-nearbyTicker.C:
			nearbyMonitoring(VivaldiClient)
		}
	}
}

func globalMonitoring() {

	// gets info from Etcd about other nodes in the area
	newNeighbors, err := GetNodesInArea(SelfRegistration.Area, false, 0)
	if err != nil {
		log.Println(err)
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	neighbors = newNeighbors

	//deletes information about servers that haven't registered anymore
	for key := range neighborInfo {
		_, ok := neighbors[key]
		if !ok {
			delete(neighborInfo, key)
		}
	}

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
	if remoteArea == "" {
		log.Printf("No remote area is configured; vertical offloading disabled")
		remoteOffloadingTarget = NodeRegistration{}
		return
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
		distanceBuf = append(distanceBuf, dist{key, VivaldiClient.DistanceTo(&s.Coordinates)})
	}
	sort.Slice(distanceBuf, func(i, j int) bool { return distanceBuf[i].distance < distanceBuf[j].distance })

	nearestNeighbors = make([]NodeRegistration, nNeighbors)
	for i := 0; i < nNeighbors; i++ {
		k := distanceBuf[i].key
		nearestNeighbors[i] = neighbors[k]
	}
}

// nearbyMonitoring check nearby server's status
func nearbyMonitoring(vivaldiClient *vivaldi.Client) {
	log.Printf("Periodic nearby Monitoring\n")

	mutex.RLock()
	// TODO: randomly choose a subset of peers for update?
	peersToUpdate := make([]NodeRegistration, 0)
	for _, reg := range neighbors {
		peersToUpdate = append(peersToUpdate, reg)
	}
	mutex.RUnlock()

	for _, registeredNode := range peersToUpdate {
		newInfo, rtt := statusInfoRequest(&registeredNode) //recupero RTT e coordinate in status info

		if newInfo == nil {
			log.Printf("Unreachable neighbor: %s\n", registeredNode.NodeID)
			continue
		}

		mutex.Lock()
		neighborInfo[registeredNode.Key] = newInfo
		neighborInfo[registeredNode.Key].LastUpdateTime = time.Now().Unix()

		//_, err := vivaldiClient.Update("node", &newInfo.Coordinates, rtt)		OLD
		_, err := vivaldiClient.Update(registeredNode.NodeID.Key, &newInfo.Coordinates, rtt) //NEW
		if err != nil {
			log.Printf("Error while updating node coordinates: %s\n", err)
		}
		mutex.Unlock()
	}

	// Updates neighborInfo with the N closest nodes from serverMap
	computeNearestNeighbors(2) //todo change this value, maybe tutti i nodi devono essere considerati (nodi stessa area)
}

func GetNearestNeighbors() []NodeRegistration {
	return nearestNeighbors
}

func GetPeerFromKey(key string) *NodeRegistration {
	mutex.RLock()
	defer mutex.RUnlock()
	reg, ok := neighbors[key]
	if !ok {
		return nil
	}
	return &reg
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
	mutex.RLock()
	defer mutex.RUnlock()
	return maps.Clone(neighborInfo)
}
