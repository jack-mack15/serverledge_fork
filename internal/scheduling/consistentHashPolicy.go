package scheduling

import (
	"fmt"
	"log"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/function"
)

type ConsistentHashPolicy struct{}

func (p *ConsistentHashPolicy) Init() {
	fallBackLocally = config.GetBool(config.SCHEDULING_FALLBACK_LOCAL, false)
	log.Printf("[INFO] Initializing EdgePolicy. Fallback to local execution set to: %t\n", fallBackLocally)
}

func (p *ConsistentHashPolicy) OnCompletion(_ *function.Function, _ *function.ExecutionReport) {

}

func (p *ConsistentHashPolicy) OnArrival(r *scheduledRequest) {
	if r.CanDoOffloading {
		fmt.Println("OFFLOADING: name " + r.Fun.Name + " runtime " + r.Fun.SupportedArchs[0])
		handleHashRingOffload(r) // This will also check for architecture compatibility
	} else {
		tryLocalExecution(r)
	}
}
