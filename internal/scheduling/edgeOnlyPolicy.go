package scheduling

import (
	"log"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
)

// EdgePolicy supports only Edge-Edge offloading. Always does offloading to an edge node if enabled. When offloading is not enabled executes the request locally.
type EdgePolicy struct{}

var fallBackLocally bool

func (p *EdgePolicy) Init() {
	fallBackLocally = config.GetBool(config.SCHEDULING_FALLBACK_LOCAL, false)
	log.Printf("[INFO] Initializing EdgePolicy. Fallback to local execution set to: %t\n", fallBackLocally)
}

func (p *EdgePolicy) OnCompletion(_ *function.Function, _ *function.ExecutionReport) {

}

func (p *EdgePolicy) OnArrival(r *scheduledRequest) {

	//tento di eseguire prima in locale
	err := tryLocalExecution(r)

	if err != nil && r.CanDoOffloading {

		url, _ := pickEdgeNodeForOffloading(r) // this will now take into account the node architecture in the offloading process
		if url != "" {
			log.Println("Risorse insufficienti: Offloading della richiesta")
			handleOffload(r, url)
			return
		}
	}

	log.Println("Dropping request")
	dropRequest(r) // r.CanDoOffloading == true, NoSuitableNode == true && fallBackLocally == false leads here, so we drop
	// the request in that case
}

func tryLocalExecution(r *scheduledRequest) error {
	if !r.Fun.SupportsArch(node.LocalNode.Arch) {
		// If the current node architecture is not supported by the function's runtime, we can only drop it, since
		// offloading was already tried unsuccessfully, or it was disabled for this request.
		dropRequest(r)
		return nil
	}

	containerID, warm, err := node.AcquireContainer(r.Fun, false)
	if err == nil {
		execLocally(r, containerID, warm)
		return nil
	}

	return err
}
