package main

import (
	"strconv"

	"github.com/serverledge-faas/serverledge/serverledge"
)

func summerHandler(params map[string]interface{}) (interface{}, error) {
	var numero int64 = 0
	volte := 0

	if val, ok := params["numero"]; ok {
		switch v := val.(type) {
		case float64:
			numero = int64(v)
		case string:
			if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
				numero = parsed
			}
		}
	}

	if val, ok := params["volte"]; ok {
		switch v := val.(type) {
		case float64:
			volte = int(v)
		case string:
			if parsed, err := strconv.Atoi(v); err == nil {
				volte = parsed
			}
		}
	}

	var risultato int64 = 0

	for i := 0; i < volte; i++ {
		risultato += numero
	}

	return risultato, nil
}

func main() {
	serverledge.Start(summerHandler)
}
