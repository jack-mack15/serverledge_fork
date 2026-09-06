package main

import (
	"fmt"

	"github.com/serverledge-faas/serverledge/serverledge"
)

// fibHandler è il punto di ingresso per Serverledge.
func fibHandler(params map[string]interface{}) (interface{}, error) {
	n := 5

	if val, ok := params["n"].(float64); ok {
		n = int(val)
	}

	if n <= 0 {
		return 0, nil
	}
	if n == 1 {
		return 1, nil
	}

	var a, b int64 = 0, 1
	for i := 2; i <= n; i++ {
		temp := a + b
		a = b
		b = temp
	}
	fmt.Println("SONO ARRIVATO A PRIMA DELLA RETURN")
	return map[string]interface{}{
		"result": b,
	}, nil
}

func main() {
	serverledge.Start(fibHandler)
}
