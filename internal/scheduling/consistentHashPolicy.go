package scheduling

import (
	"fmt"
	"log"

	"github.com/pkg/errors"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
)

type ConsistentHashPolicy struct{}

func (p *ConsistentHashPolicy) Init() {
	fallBackLocally = config.GetBool(config.SCHEDULING_FALLBACK_LOCAL, false)
	log.Printf("[INFO] Initializing EdgePolicy. Fallback to local execution set to: %t\n", fallBackLocally)
}

func (p *ConsistentHashPolicy) OnCompletion(f *function.Function, report *function.ExecutionReport) {
}

func (p *ConsistentHashPolicy) OnArrival(r *scheduledRequest) {
	log.Println("ON ARRIVAL SUBITO")
	if r.offloaded {
		//qualche nodo ha designato me come nodo per la richiesta
		err := tryLocalExecutionConsistentHash(r)
		if err == nil {
			return
		}

		if errors.Is(err, node.OutOfResourcesErr) && r.CanDoOffloading {
			//non ho risorse per gestirla, la mando al prossimo sull'anello, last chance
			log.Println("LAST CHANCEEEEEE")
			handleLastChanceOffload(r)
			return

		} else {
			//non ho scelta, la scarto
			log.Println("DROP: Dropping request " + r.Fun.Name)
			dropRequest(r)
			return
		}
	} else {
		//prima volta che la richiesta viene schedlata
		if r.CanDoOffloading {
			r.offloaded = true
			//in questo modo il prossimo nodo deve gestirla
			fmt.Println("OFFLOADING: name " + r.Fun.Name + " runtime " + r.Fun.SupportedArchs[0])
			handleHashRingOffload(r) // This will also check for architecture compatibility
			return
		}
	}
	//se non l'ho già gestita, la gestisco io
	_ = tryLocalExecutionConsistentHash(r)
}

func tryLocalExecutionConsistentHash(r *scheduledRequest) error {
	log.Println("LOCAL EXEC: try local execution")
	if !r.Fun.SupportsArch(node.LocalNode.Arch) {
		//should not happen
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
