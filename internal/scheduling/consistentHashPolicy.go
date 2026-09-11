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

func (p *ConsistentHashPolicy) OnCompletion(f *function.Function, report *function.ExecutionReport) {
}

func (p *ConsistentHashPolicy) OnArrival(r *scheduledRequest) {
	if r.CanDoOffloading {
		//in questo modo il prossimo nodo deve gestirla
		fmt.Println("OFFLOADING: name " + r.Fun.Name + " runtime " + r.Fun.SupportedArchs[0])
		handleHashRingOffload(r) // This will also check for architecture compatibility
	} else {
		tryLocalExecution(r)
	}
}
