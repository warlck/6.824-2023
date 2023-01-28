package mr

import (
	"fmt"
	"os"
)

func debugf(format string, params ...interface{}) {
	debug := os.Getenv("DEBUG")
	if debug != "" {
		fmt.Println("=============  DEBUG ==============")
		fmt.Printf(format, params...)
	}
}
