package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/serverledge-faas/serverledge/internal/mab"
	"github.com/serverledge-faas/serverledge/internal/node"

	"golang.org/x/net/context"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/lb"
	"github.com/serverledge-faas/serverledge/internal/registration"
)

func registerTerminationHandler(e *echo.Echo) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt)

	go func() {
		select {
		case sig := <-c:
			fmt.Printf("Got %s signal. Terminating...\n", sig)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := e.Shutdown(ctx); err != nil {
				e.Logger.Fatal(err)
			}

			os.Exit(0)
		}
	}()
}

func main() {
	configFileName := ""
	if len(os.Args) > 1 {
		configFileName = os.Args[1]
	}
	config.ReadConfiguration(configFileName)

	myArea := config.GetString(config.REGISTRY_AREA, "ROMA")
	node.LocalNode = node.NewRandomIdentifier(myArea)

	err := registration.RegisterLoadBalancer()
	if err != nil {
		log.Fatal(err)
	}

	//creo l'area cloud
	err = registration.CreateNewArea(myArea)
	if err != nil {
		log.Fatal(err)
	}

	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())

	// Register a signal handler to clea-nup things on termination
	registerTerminationHandler(e)

	//avvio il registration per pharos
	err = registration.StartMonitoring()
	if err != nil {
		log.Fatal(err)
	}

	mab.InitBanditManager()
	lb.StartReverseProxy(e, myArea)
}
