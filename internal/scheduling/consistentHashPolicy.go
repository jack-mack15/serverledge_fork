package scheduling

import (
	"log"

	"github.com/pkg/errors"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
)

type ConsistentHashPolicy struct{}

func (p *ConsistentHashPolicy) Init() {
	fallBackLocally = config.GetBool(config.SCHEDULING_FALLBACK_LOCAL, false)
	log.Printf("[INFO] Initializing ConsistentHashPolicy\n")
}

func (p *ConsistentHashPolicy) OnCompletion(f *function.Function, report *function.ExecutionReport) {
}

func (p *ConsistentHashPolicy) OnArrival(r *scheduledRequest) {
	if r.offloaded {
		//qualche nodo ha designato me come nodo per la richiesta
		err := tryLocalExecutionConsistentHash(r)
		if err == nil {
			return
		}

		if errors.Is(err, node.OutOfResourcesErr) && r.CanDoOffloading {
			//non ho risorse per gestirla, la mando al prossimo sull'anello, last chance
			handleLastChanceOffload(r)
			return

		} else {
			//non ho scelta, la scarto
			log.Println("CHP: dropping request " + r.Fun.Name)
			dropRequest(r)
			return
		}
	} else {
		//prima volta che la richiesta viene schedlata
		if r.CanDoOffloading {
			r.offloaded = true
			//in questo modo il prossimo nodo deve gestirla
			log.Println("CHP: offloading to name " + r.Fun.Name + " runtime " + r.Fun.SupportedArchs[0])
			handleHashRingOffload(r) // This will also check for architecture compatibility
			return
		}
	}
	//se non l'ho già gestita, la gestisco io
	_ = tryLocalExecutionConsistentHash(r)
}

func tryLocalExecutionConsistentHash(r *scheduledRequest) error {
	if !r.Fun.SupportsArch(node.LocalNode.Arch) {
		//should not happen
		dropRequest(r)
		return nil
	}

	containerID, warm, err := node.AcquireContainer(r.Fun, false)
	if err == nil {
		log.Println("CHP: local execution")
		registration.UpdateResources(node.LocalNode.Key, r.Fun.MemoryMB, r.Fun.CPUDemand, true)
		execLocally(r, containerID, warm)
		registration.UpdateResources(node.LocalNode.Key, r.Fun.MemoryMB, r.Fun.CPUDemand, false)
		return nil
	}

	return err
}
