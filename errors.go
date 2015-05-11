package main

import "fmt"
import "os"
import "runtime/debug"

func check(err error) {
	if err != nil {
		fmt.Println(err)
		debug.PrintStack()
		os.Exit(-1)
	}
}
