package registration

import (
	"errors"
	"time"

	"github.com/hexablock/vivaldi"
	"github.com/serverledge-faas/serverledge/internal/node"
)

var UnavailableClientErr = errors.New("etcd client unavailable")
var IdRegistrationErr = errors.New("etcd error: could not complete the registration")

type NodeRegistration struct {
	node.NodeID
	IPAddress      string
	APIPort        int
	UDPPort        int
	IsLoadBalancer bool
}

type StatusInformation struct {
	AvailableWarmContainers map[string]int // <k, v> = <function name, warm container number>
	TotalMemory             int64
	AvailableMemory         int64
	FreeMemory              int64
	TotalCPU                float64
	UsedCPU                 float64
	Coordinates             vivaldi.Coordinate
	LoadAvg                 []float64
	LastUpdateTime          int64 // timestamp of last update of this information
}

type AnchorResponse struct {
	Type        byte
	Coordinates vivaldi.Coordinate
	Radius      int64
}

type GeneralRequest struct {
	Type   byte
	NodeId string
}

type ArchAPIResponse struct {
	Arch    string
	APIPort int
}

type FailureInfo struct {
	NodeKey   string
	DeadTimes int
	LastSeen  int64
}

func (f *FailureInfo) NodeAlive() {
	f.DeadTimes = 0
	f.LastSeen = time.Now().UnixMilli()
}

func (f *FailureInfo) NodeDead() {
	f.DeadTimes++
}
