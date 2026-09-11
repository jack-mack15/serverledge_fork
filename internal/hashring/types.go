package hashring

import (
	"log"
	"sync"
	"time"

	"github.com/labstack/echo/v4/middleware"
	"github.com/serverledge-faas/serverledge/internal/function"
)

var AllMemoryAvailable = int64(10_000_000) // A high value to symbolize all memory is free

type HashRingTarget struct {
	NodeKey  string
	HopNumb  int
	Distance time.Duration
}

// MemoryChecker is the function that checks if the node selected has enough memory to execute the function.
// it is an interface, and it's put in HashRing to make unit-tests possible by mocking it
type MemoryChecker interface {
	HasEnoughMemory(target *middleware.ProxyTarget, fun *function.Function) bool
}

type DefaultMemoryChecker struct{}

func (m *DefaultMemoryChecker) HasEnoughMemory(candidate *middleware.ProxyTarget, fun *function.Function) bool {
	freeMemoryMB := NodeMetrics.GetFreeMemory(candidate.Name)
	freeCpu := NodeMetrics.metrics[candidate.Name].FreeCPU
	log.Printf("Candidate has: %d MB free memory. Function needs: %d MB", freeMemoryMB, fun.MemoryMB)
	return freeMemoryMB >= fun.MemoryMB && freeCpu >= fun.CPUDemand

}

var NodeMetrics = &NodeMetricCache{
	metrics: make(map[string]NodeMetric),
}

type NodeMetric struct {
	TotalMemoryMB int64
	FreeMemoryMB  int64
	LastUpdate    int64
	TotalCPU      float64
	FreeCPU       float64
}

type NodeMetricCache struct {
	mu      sync.RWMutex
	metrics map[string]NodeMetric
}

// Update info about memory of a specific node. If totalMemMB = 0, then we keep the previous value.
func (c *NodeMetricCache) Update(nodeName string, freeMemMB int64, totalMemMB int64, updateTime int64, freeCpu float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	curr, ok := c.metrics[nodeName]
	if ok && (updateTime < curr.LastUpdate) {
		return // if this branch is taken, we do not update. The info we already have is "fresher" than the one we received now
	}

	if totalMemMB == 0 && ok {
		totalMemMB = curr.TotalMemoryMB
	}

	c.metrics[nodeName] = NodeMetric{
		TotalMemoryMB: totalMemMB,
		FreeMemoryMB:  freeMemMB,
		LastUpdate:    updateTime,
		FreeCPU:       freeCpu,
	}
}

func (c *NodeMetricCache) GetFreeMemory(nodeName string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	val, ok := c.metrics[nodeName]
	if !ok {

		return AllMemoryAvailable
	}

	return val.FreeMemoryMB
}

func (c *NodeMetricCache) GetCpu(nodeName string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	val, ok := c.metrics[nodeName]
	if !ok {
		return 0
	}

	return val.FreeCPU
}
