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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
