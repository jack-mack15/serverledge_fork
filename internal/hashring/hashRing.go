package hashring

import (
	"fmt"
	"hash/fnv"
	"log"
	"sort"
	"sync"

	"github.com/labstack/echo/v4/middleware"
	"github.com/serverledge-faas/serverledge/internal/function"
)

// questa mappa serve per indicare i nodi che attulmente sembrano essere offline
var offlineNodes map[string]struct{}
var offlineMutex sync.RWMutex

type HashRing struct {
	replicas int
	ring     []uint32                           // actual ring with hash of nodes
	targets  map[uint32]*middleware.ProxyTarget // mapping hash(es) <-> node. Each node will have #replicas entries in the ring
	// function to check if the node selected has enough memory to execute the function
	TargetList []*middleware.ProxyTarget // list of target. Cached instead of iterating on targets every time.
	memChecker MemoryChecker             // implemented this way to make the code testable by mocking this struct/function.

}

func NewHashRing(replicas int) *HashRing {

	return &HashRing{
		replicas:   replicas,
		ring:       make([]uint32, 0),
		targets:    make(map[uint32]*middleware.ProxyTarget),
		TargetList: make([]*middleware.ProxyTarget, 0),
		memChecker: &DefaultMemoryChecker{},
	}
}

func (r *HashRing) Add(t *middleware.ProxyTarget) {
	// put replicas in the ring. To do so we'll hash the node's name + an incrementing number
	for i := 0; i < r.replicas; i++ {
		key := fmt.Sprintf("%s#%d", t.Name, i)
		h := hash(key)
		r.ring = append(r.ring, h)
		r.targets[h] = t
	}
	sort.Slice(r.ring, func(i, j int) bool { return r.ring[i] < r.ring[j] }) // sort the ring by hash
	r.TargetList = append(r.TargetList, t)
}

func (r *HashRing) Get(fun *function.Function) *middleware.ProxyTarget {
	if len(r.ring) == 0 {
		return nil
	}

	h := hash(fun.Name)
	// we'll return the node whose hash is the next in the ring, starting from the hash of the function's name
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= h })
	if idx == len(r.ring) {
		idx = 0
	}
	candidate := r.targets[r.ring[idx]] // here we use the map to get the node corresponding to the hash

	if r.memChecker.HasEnoughMemory(candidate, fun) && fun.SupportsArch(candidate.Meta["arch"].(string)) {
		return candidate
	}

	// If the node found by consistent hashing doesn't have enough memory, we'll try to find another node to execute the function
	// by navigating the ring, so that the lookup order is consistent between different executions (if the nodes' pool doesn't change
	// of course).
	startingIdx := idx            // idx is still set to the candidate index in the ring
	idx = (idx + 1) % len(r.ring) // next node in the ring

	// since there are multiple replicas of every physical node, I'll keep track of nodes already considered as candidates
	// to skip them and make the lookup faster.
	// E.g.: if the first candidate was node "Node-A", found by its replica "Node-A#1", there is no point in trying to see
	// if "Node-A" has enough memory once I find it through its replica "Node-A#2", for example, that I may find while
	// traversing the ring.
	seen := make(map[string]struct{})
	seen[candidate.Name] = struct{}{} // I already tried it

	for idx != startingIdx { // as long as I have not completed a full circle
		candidate = r.targets[r.ring[idx]]     // new candidate: idx is the replica's index. candidate is the corresponding physical node
		_, alreadySeen := seen[candidate.Name] // I check if it's in the map (meaning I already tried it)

		if !alreadySeen && r.memChecker.HasEnoughMemory(candidate, fun) && fun.SupportsArch(candidate.Meta["arch"].(string)) {
			return candidate
		} else {
			seen[candidate.Name] = struct{}{} // it's a map, it doesn't really matter if alreadySeen was true or not, there are no duplicates
			idx = (idx + 1) % len(r.ring)
		}
	}

	return nil // no suitable node found

}

// variante di Get che ritorna i primi max elementi successivi (e disponibili) data una funzione
func (r *HashRing) GetMultiple(fun *function.Function, max int) []HashRingTarget {
	if len(r.ring) == 0 {
		return nil
	}

	if len(r.ring) < max {
		max = len(r.ring)
	}

	h := hash(fun.Name)
	idx := sort.Search(len(r.ring), func(i int) bool { return r.ring[i] >= h })
	if idx == len(r.ring) {
		idx = 0
	}

	startingIdx := idx
	//per le repliche di nodi già visti
	seen := make(map[string]struct{})

	var targets []HashRingTarget

	hopCount := 0

	for {
		candidate := r.targets[r.ring[idx]]
		_, alreadySeen := seen[candidate.Name]
		if !alreadySeen {
			//incremento hop count poichè incontro un nuovo nodo
			hopCount++

			seen[candidate.Name] = struct{}{}

			//test se ha sufficiente memoria
			if r.memChecker.HasEnoughMemory(candidate, fun) && !checkOfflineNode(candidate.Name) {
				temp := HashRingTarget{
					NodeKey:  candidate.Name,
					HopNumb:  hopCount,
					Distance: 0,
				}
				targets = append(targets, temp)

				//controllo se ho preso i primi max nodi
				if len(targets) == max {
					break
				}
			}
		}

		//incremento con attenzione al wrap around
		idx = (idx + 1) % len(r.ring)

		//controllo di sicurezza per uscire se vedo tutto l'anello
		if idx == startingIdx {
			break
		}
	}

	return targets
}

func InitOfflineNodes() {
	offlineNodes = make(map[string]struct{})
}

func AddOfflineSuspect(nodeKey string) {
	offlineMutex.Lock()
	defer offlineMutex.Unlock()
	offlineNodes[nodeKey] = struct{}{}
}

func RemoveOfflineSuspect(nodeKey string) {
	offlineMutex.Lock()
	defer offlineMutex.Unlock()
	delete(offlineNodes, nodeKey)
}

func checkOfflineNode(nodeKey string) bool {
	offlineMutex.RLock()
	defer offlineMutex.RUnlock()
	if _, ok := offlineNodes[nodeKey]; ok {
		return true
	}
	return false
}

func (r *HashRing) RemoveByName(name string) bool {
	removed := false
	newRing := make([]uint32, 0)

	// We'll delete all entries for this node from the targets' map, and generate a new ring without them.

	for _, h := range r.ring {
		if r.targets[h].Name == name {
			delete(r.targets, h)
			removed = true
		} else {
			newRing = append(newRing, h)
		}
	}

	if removed {
		r.ring = newRing
		sort.Slice(r.ring, func(i, j int) bool { return r.ring[i] < r.ring[j] })
		r.removeFromTargetList(name)
		if checkOfflineNode(name) {
			offlineMutex.Lock()
			delete(offlineNodes, name)
			offlineMutex.Unlock()
		}

	}

	return removed
}

func (r *HashRing) removeFromTargetList(targetName string) {
	newList := r.TargetList[:0]
	for _, t := range r.TargetList {
		if t.Name != targetName {
			newList = append(newList, t)
		}
	}
	r.TargetList = newList
}

// Size returns the number of UNIQUE nodes in the ring, not the numbers of total nodes (which is = nUniqueNodes * Replicas)
func (r *HashRing) Size() int {

	return len(r.TargetList)

}

func (r *HashRing) GetAllTargets() []*middleware.ProxyTarget {
	return r.TargetList
}

// hash function uses the FNV-1a function. It has good distribution and is fast to compute. It's not cryptographically safe,
// but should be good enough for our purposes (consistent-hashing).
func hash(s string) uint32 {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		log.Printf("error hashing %s: %v", s, err)
	}
	return h.Sum32()
}
