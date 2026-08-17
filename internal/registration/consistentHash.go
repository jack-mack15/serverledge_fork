package registration

import (
	"fmt"
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
			return
		}
		archMap := echo.Map{"arch": n.Arch}
		target := &middleware.ProxyTarget{Name: n.Key, URL: parsedUrl, Meta: archMap}

		//todo verificare le stringhe corrette
		ring, _ := getRingByArch(n.Arch)
		ring.Add(target)

	}
}

// RemoveNode rimuove un elemento dalla mappa di nodi e dall'hash ring
func RemoveNode(nodeKey string, arch string) {

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
	fmt.Println("Consistent Hash Insert Node: " + node.Key + "  with arch: " + node.Arch)
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

/*
// questa funzione cerca di unire il concetto di hash ring con il concetto di distanza
func GetTargetNodeBest(f *function.Function) *StatusInformation {
	var bestNodeHash []*StatusInformation

	mutexHash.RLock()

	length := len(hashRing)

	if length == 0 {
		log.Printf("impossibile instradare '%s': l'anello è vuoto", f.Name)
		return nil
	}

	//hash del nome della funzione
	funcHash := hash(f.Name)

	//scorro anello e trovo il primo nodo successivo a funcHash
	idx := sort.Search(length, func(i int) bool {
		return hashRing[i] >= funcHash
	})

	//significa che il primo nodo successivo è il primo
	if idx == length {
		idx = 0
	}
	currMax := 0
	//idx sarà il nodo di partenza, da questo scorriamo il ring fino a trovare un nodo che può eseguire la funzione
	for i := 0; i < length; i++ {
		tempHash := hashRing[idx]
		tempNode := hashNodes[tempHash]
		if testResources(f, GetPeerFromKey(tempNode.Key), GetStatusInfoFromKey(tempNode.Key)) {
			bestNodeHash = append(bestNodeHash, GetStatusInfoFromKey(tempNode.Key))
			currMax++
		}
		idx = (idx + 1) % length
		i++
		if currMax == MAX {
			break
		}
	}

	mutexHash.RUnlock()

	//controllo se non ci sono nodi
	if len(bestNodeHash) == 0 {
		log.Println("Hash Ring Error: no node can process the function")
		return nil
	}

	//a questo punto dovrei aver ottenuto i migliori nodi (quelli disponibili e vicini all'hash della funizone)
	var bestScore = math.MaxInt
	var bestNode *StatusInformation
	for i := 0; i < len(bestNodeHash); i++ {
		tempDist := LocalVivaldiClient.DistanceTo(&bestNodeHash[i].Coordinates).Milliseconds()
		tempScore := (int)(tempDist*30) + i*70
		if tempScore <= bestScore {
			bestScore = tempScore
			bestNode = bestNodeHash[i]
		}
	}

	return bestNode
}

// funzione che verifica se il nodo scelto può gestire tale funzione
func testResources(f *function.Function, node *NodeRegistration, info *StatusInformation) bool {
	if f.SupportsArch(node.Arch) && info.AvailableMemory > f.MemoryMB {
		return true
	}
	return false
}

// Hash function uses the FNV-1a function. It has good distribution and is fast to compute. It's not cryptographically safe,
// but should be good enough for our purposes (consistent-hashing).
func hash(s string) uint32 {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		log.Printf("error hashing %s: %v", s, err)
	}
	return h.Sum32()
}*/
