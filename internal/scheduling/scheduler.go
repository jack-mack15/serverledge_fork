package scheduling

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/registration"

	"github.com/serverledge-faas/serverledge/internal/container"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/metrics"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/telemetry"

	"go.opentelemetry.io/otel/trace"
)

var requests chan *scheduledRequest
var completions chan *completionNotification

var offloadingClient *http.Client

func Run(p Policy) {
	requests = make(chan *scheduledRequest, 500)
	completions = make(chan *completionNotification, 500)

	node.LocalResources.Init()
	log.Printf("Current resources: %v\n", &node.LocalResources)

	container.InitDockerContainerFactory()

	//janitor periodically remove expired warm container
	node.GetJanitorInstance()

	tr := &http.Transport{
		MaxIdleConns:        2500,
		MaxIdleConnsPerHost: 2500,
		MaxConnsPerHost:     0,
		IdleConnTimeout:     30 * time.Minute,
	}
	offloadingClient = &http.Client{Transport: tr}

	// initialize scheduling policy
	p.Init()

	log.Println("Scheduler started.")

	var r *scheduledRequest
	var c *completionNotification
	for {
		select {
		case r = <-requests: // receive request
			go p.OnArrival(r)
		case c = <-completions:
			f, found := function.GetFunction(c.funcName)
			if !found {
				log.Printf("Function %s not found", c.funcName)
				continue
			}
			node.HandleCompletion(c.cont, f)
			p.OnCompletion(f, &c.report)

			if metrics.Enabled && !c.failed {
				metrics.AddCompletedInvocation(c.funcName, !c.report.IsWarmStart)
				if !c.offloaded {
					metrics.AddFunctionDurationValue(c.funcName, c.report.Duration)
					if !c.report.IsWarmStart {
						metrics.AddFunctionInitTimeValue(c.funcName, c.report.InitTime)
					}
				}
				outputSize := len(c.report.Result)
				metrics.AddFunctionOutputSizeValue(r.Fun.Name, float64(outputSize))
			}
		}
	}

}

// SubmitRequest submits a newly arrived request for scheduling and execution
func SubmitRequest(r *function.Request) (*function.ExecutionReport, error) {
	schedRequest := scheduledRequest{
		Request:         r,
		ExecutionReport: &function.ExecutionReport{},
		decisionChannel: make(chan schedDecision, 1)}
	requests <- &schedRequest

	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Scheduling start")
	}

	// wait on channel for scheduling action
	schedDecision, ok := <-schedRequest.decisionChannel
	if !ok {
		return nil, fmt.Errorf("could not schedule the request")
	}
	//log.Printf("[%s] Scheduling decision: %v", r, schedDecision)

	if telemetry.DefaultTracer != nil {
		trace.SpanFromContext(r.Ctx).AddEvent("Scheduling complete")
	}

	if schedDecision.action == DROP {
		//log.Printf("[%s] Dropping request", r)
		return nil, node.OutOfResourcesErr
	} else if schedDecision.action == EXEC_REMOTE {
		//log.Printf("Offloading request")
		err := Offload(&schedRequest, schedDecision.remoteHost)
		return schedRequest.ExecutionReport, err
	} else {
		err := Execute(schedDecision.cont, &schedRequest, schedDecision.useWarm)
		return schedRequest.ExecutionReport, err
	}
}

// SubmitAsyncRequest submits a newly arrived async request for scheduling and execution
func SubmitAsyncRequest(r *function.Request) {
	schedRequest := scheduledRequest{
		Request:         r,
		ExecutionReport: &function.ExecutionReport{},
		decisionChannel: make(chan schedDecision, 1)}
	requests <- &schedRequest // send async request

	// wait on channel for scheduling action
	schedDecision, ok := <-schedRequest.decisionChannel
	if !ok {
		publishAsyncResponse(r.Id(), function.Response{Success: false})
		return
	}

	var err error
	if schedDecision.action == DROP {
		publishAsyncResponse(r.Id(), function.Response{Success: false})
	} else if schedDecision.action == EXEC_REMOTE {
		//log.Printf("Offloading request\n")
		err = OffloadAsync(r, schedDecision.remoteHost)
		if err != nil {
			publishAsyncResponse(r.Id(), function.Response{Success: false})
		}
	} else {
		err = Execute(schedDecision.cont, &schedRequest, schedDecision.useWarm)
		if err != nil {
			publishAsyncResponse(r.Id(), function.Response{Success: false})
			return
		}
		publishAsyncResponse(r.Id(), function.Response{Success: true, ExecutionReport: *schedRequest.ExecutionReport})
	}
}

func dropRequest(r *scheduledRequest) {
	r.decisionChannel <- schedDecision{action: DROP}
}

func execLocally(r *scheduledRequest, c *container.Container, warmStart bool) {
	decision := schedDecision{action: EXEC_LOCAL, cont: c, useWarm: warmStart}
	r.decisionChannel <- decision
}

func handleOffload(r *scheduledRequest, serverHost string) {
	r.CanDoOffloading = false // the next server can't offload this request
	r.decisionChannel <- schedDecision{
		action:     EXEC_REMOTE,
		cont:       nil,
		remoteHost: serverHost,
	}
}

func consistentHashOffload(r *scheduledRequest, serverHost string) {
	r.CanDoOffloading = true // the next server is the last one to offload if necessary
	r.decisionChannel <- schedDecision{
		action:     EXEC_REMOTE,
		cont:       nil,
		remoteHost: serverHost,
	}
}

func handleCloudOffload(r *scheduledRequest) {
	offloadingTarget := registration.GetRemoteOffloadingTarget()
	if offloadingTarget == nil {
		log.Printf("No remote offloading target available; dropping request")
		r.decisionChannel <- schedDecision{action: DROP}
		// TODO check if this is a correct assumption to make
	} else if offloadingTarget.IsLoadBalancer || r.Fun.SupportsArch(node.LocalNode.Arch) {
		handleOffload(r, offloadingTarget.APIUrl())
	} else {
		dropRequest(r)
	}
}

func handleHashRingOffload(r *scheduledRequest) {
	//cerco numero di nodi target dall'hash ring. il numero di questi nodi è pari a hash.ring.targets o 5
	hashRingTargets, maxDistance, maxHop := registration.GetTargetsFromHashRing(r.Fun)
	//se distanze ancora non sono impostate
	if maxDistance == 0 {
		maxDistance = 1
	}
	if maxHop == 0 {
		maxHop = 1
	}
	if hashRingTargets == nil {
		log.Println("------------------------hashringTargets è vuoto")
	}

	var bestNode registration.NodeRegistration
	if len(hashRingTargets) == 0 {
		//se hash ring non trova, opto per il cloud. se non si riesce con il cloud, effettuo il drop
		log.Println("Consistent Hash: offloading in cloud")
		handleCloudOffload(r)
		return
	}

	//calcolo del punteggio
	weight := config.GetFloat(config.CONSISTENT_HASH_WEIGHT, 0.5)
	var best int
	bestPoints := 1.0
	for index, elem := range hashRingTargets {
		currPoints := (1.0 - weight) * (float64(elem.Distance.Milliseconds()) / float64(maxDistance.Milliseconds()))
		currPoints += weight * float64(elem.HopNumb) / float64(maxHop)
		//log.Printf("TEST in selection, points: %f, hops: %d, distance: %d\n", currPoints, elem.HopNumb, elem.Distance.Milliseconds())
		if currPoints < bestPoints {
			best = index
			bestPoints = currPoints
		}
	}

	bestNode = *registration.GetPeerFromKey(hashRingTargets[best].NodeKey)
	
	//il nodo corrente deve gestire l'esecuzione
	if bestNode.Key == node.LocalNode.Key {
		registration.UpdateResources(bestNode.Key, r.Fun.MemoryMB, r.Fun.CPUDemand, true)
		containerID, warm, err := node.AcquireContainer(r.Fun, false)
		if err == nil {
			log.Println("Consistent Hash: execution locally")
			execLocally(r, containerID, warm)
			registration.UpdateResources(bestNode.Key, r.Fun.MemoryMB, r.Fun.CPUDemand, false)
			return
		}

		return
	}
	log.Println("Consistent Hash: offloading in edge")
	consistentHashOffload(r, bestNode.APIUrl())
}

func handleLastChanceOffload(r *scheduledRequest) {
	target := registration.GetLastChanceTarget(r.Fun, node.LocalNode.Key)
	if target != nil {
		//mando la richiesta a questo nodo
		host := registration.GetPeerFromKey(target.Name)
		handleOffload(r, host.APIUrl())
	} else {
		dropRequest(r)
	}
}
