package registration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetUpRing(t *testing.T) {
	nodes := make(map[string]NodeRegistration)
	//test senza nodi
	SetUpRing(nodes)

	hash := GetConsistentHashRing()

	assert.NotNil(t, hash.x86Ring)
	assert.NotNil(t, hash.armRing)

	//test con un nodo arm
	armNode := NodeRegistration{
		IPAddress:      "0.0.0.0",
		APIPort:        0,
		UDPPort:        0,
		IsLoadBalancer: false,
	}
	armNode.Key = "test"
	armNode.Area = "test"
	armNode.Arch = "arm64"

	nodes["test"] = armNode

	SetUpRing(nodes)

	hash = GetConsistentHashRing()

	assert.NotNil(t, hash.armRing)
	assert.NotNil(t, hash.armRing)

	assert.NotNil(t, hash.armRing.TargetList[0])

	//test con un nodo arm e uno amd
	amdNode := NodeRegistration{
		IPAddress:      "0.0.0.0",
		APIPort:        0,
		UDPPort:        0,
		IsLoadBalancer: false,
	}
	amdNode.Key = "test2"
	amdNode.Area = "test"
	amdNode.Arch = "amd64"

	nodes["test2"] = amdNode

	SetUpRing(nodes)

	hash = GetConsistentHashRing()

	assert.NotNil(t, hash.x86Ring)
	assert.NotNil(t, hash.x86Ring)

	assert.NotNil(t, hash.x86Ring.TargetList[0])
}

func TestInsertNode(t *testing.T) {
	nodes := make(map[string]NodeRegistration)

	SetUpRing(nodes)

	amdNode := NodeRegistration{
		IPAddress:      "0.0.0.0",
		APIPort:        0,
		UDPPort:        0,
		IsLoadBalancer: false,
	}
	amdNode.Key = "test2"
	amdNode.Area = "test"
	amdNode.Arch = "amd64"

	InsertNodeHash(amdNode)

	hash := GetConsistentHashRing()

	assert.NotNil(t, hash.x86Ring)
	assert.NotNil(t, hash.x86Ring.TargetList[0])
	assert.Equal(t, "test2", hash.x86Ring.TargetList[0].Name)
}

func TestRemoveNode(t *testing.T) {

}

func TestGetNewAnchor(t *testing.T) {

}

func TestGetTargetsFromHashRing(t *testing.T) {

}
