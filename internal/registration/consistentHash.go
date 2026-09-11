package registration

import (
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/hashring"
)

type CompleteHashRing struct {
	x86Mu sync.RWMutex
	armMu sync.RWMutex

	armRing *hashring.HashRing
	x86Ring *hashring.HashRing
}

var localHashRing CompleteHashRing

// SetUpRing inizializza l'hash ring a partire da una map di tutti i nodi dell'area
func SetUpRing(nodes map[string]NodeRegistration) {

	REPLICAS := config.GetInt(config.REPLICAS, 128)
	log.Printf("Running Consistent Hashing with %d replicas per node in the hash rings\n", REPLICAS)
	localHashRing.armRing = hashring.NewHashRing(REPLICAS)
	localHashRing.x86Ring = hashring.NewHashRing(REPLICAS)
	hashring.InitOfflineNodes()
	if len(nodes) == 0 {
		return
	}
	for _, n := range nodes {
		parsedUrl, err := url.Parse(n.APIUrl())
		if err != nil {
			log.Printf("SetUpRing in Consistent Hash: Error parsing URL: %v\n", err)
			continue
		}
		archMap := echo.Map{"arch": n.Arch}
		log.Println("SetUpRing architecture added is: " + n.Arch)
		target := &middleware.ProxyTarget{Name: n.Key, URL: parsedUrl, Meta: archMap}

		ring, _ := getRingByArch(n.Arch)
		if ring != nil {
			ring.Add(target)
		}
	}
}

// ConsistentHashRemoveNode rimuove un elemento dalla mappa di nodi e dall'hash ring
func ConsistentHashRemoveNode(nodeKey string, arch string) {

	//per rendere estensibile
	ring, mu := getRingByArch(arch)

	if mu == nil {
		log.Printf("Consistent Hash: Architettura non supportata: %s\n", arch)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	ring.RemoveByName(nodeKey)
}

// InsertNodeHash aggiunge un nuovo nodo alle strutture dati per il consistent hashing
func InsertNodeHash(node NodeRegistration) {
	log.Println("Consistent Hash Insert Node: " + node.Key + "  with arch: " + node.Arch)
	//creazione proxy target
	parsedUrl, err := url.Parse(node.APIUrl())
	if err != nil {
		log.Printf("SetUpRing in Consistent Hash: Error parsing URL: %v\n", err)
		return
	}
	archMap := echo.Map{"arch": node.Arch}
	target := &middleware.ProxyTarget{Name: node.Key, URL: parsedUrl, Meta: archMap}

	//per rendere estensibile
	ring, mu := getRingByArch(node.Arch)

	if mu == nil {
		log.Printf("Consistent Hash: Architettura non supportata: %s\n", node.Arch)
		return
	}

	mu.Lock()
	defer mu.Unlock()

	ring.Add(target)

	//aggiunta delle metriche
	nodeInfo := GetStatusInfoFromKey(node.Key)
	if nodeInfo != nil {
		hashring.NodeMetrics.Update(node.Key, nodeInfo.AvailableMemory, nodeInfo.TotalMemory, nodeInfo.LastUpdateTime,
			nodeInfo.TotalCPU-nodeInfo.UsedCPU)
	}
}

// GetTargetsFromHashRing ritorna il nodo che sul ring gestisce la funzione specificata.
// sta attento anche all'utilizzo delle risorse
func GetTargetsFromHashRing(f *function.Function) ([]hashring.HashRingTarget, time.Duration, int) {
	var maxHop int
	var maxDistance time.Duration

	//scorro su ogni architettura supportata dalla funzione
	for _, arch := range f.SupportedArchs {
		ring, mu := getRingByArch(arch)
		if ring == nil {
			continue
		}
		mu.RLock()
		targets := ring.GetMultiple(f, config.GetInt(config.HASH_RING_TARGETS, 5))
		mu.RUnlock()
		//riempo il campo distance delle strutture HashRingTarget
		//calcolo anche la distanza massima e il numero di hop massimi

		for _, elem := range targets {
			temp := GetStatusInfoFromKey(elem.NodeKey)
			if temp != nil {
				elem.Distance = CalculateDistanceTo(&temp.Coordinates)
			} else {
				//se non ottengo info di status la distanza la imposto manualmente
				elem.Distance = 1000
			}
			//calcolo valori massimi di hop e distanza
			if elem.HopNumb > maxHop {
				maxHop = elem.HopNumb
			}
			if elem.Distance > maxDistance {
				maxDistance = elem.Distance
			}
		}
		return targets, maxDistance, maxHop
	}
	return nil, maxDistance, maxHop
}

// ritorna il primo elemento dello stesso ring in cui era situata la vecchia anchor.
// se tale ring è vuoto, prende il primo nodo dall'altro ring.
func GetNewAnchor(arch string) string {
	var newAnchor string
	ring, mu := getRingByArch(arch)
	//recupero il primo nodo dall'anello in cui era la prima anchor
	if ring != nil {
		mu.RLock()
		newAnchor = ring.GetFirstNode()
		mu.RUnlock()
	}
	//se non trovo nulla, significa che il primo anello adesso è vuoto, cerco nel prossimo
	if newAnchor == "" {
		ring, mu = getReverseRingByArch(arch)
		mu.RLock()
		newAnchor = ring.GetFirstNode()
		mu.RUnlock()
	}
	return newAnchor
}

func UpdateResources(node string, memory int64, cpu float64) {
	freeMemMB := hashring.NodeMetrics.GetFreeMemory(node) - memory
	freeCpu := hashring.NodeMetrics.GetCpu(node) - cpu
	hashring.NodeMetrics.Update(node, freeMemMB, 0, time.Now().Unix(), freeCpu)
}

func getRingByArch(arch string) (*hashring.HashRing, *sync.RWMutex) {
	switch arch {
	case "arm64":
		return localHashRing.armRing, &localHashRing.armMu
	case "amd64":
		return localHashRing.x86Ring, &localHashRing.x86Mu
	default:
		log.Printf("Consistent Hash: Architettura non supportata: %s\n", arch)
		return nil, nil
	}
}

func getReverseRingByArch(arch string) (*hashring.HashRing, *sync.RWMutex) {
	switch arch {
	case "amd64":
		return localHashRing.armRing, &localHashRing.armMu
	case "arm64":
		return localHashRing.x86Ring, &localHashRing.x86Mu
	default:
		log.Printf("Consistent Hash: Architettura non supportata: %s\n", arch)
		return nil, nil
	}
}
