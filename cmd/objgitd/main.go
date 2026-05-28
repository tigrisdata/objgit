package main

import (
	"flag"

	"github.com/facebookgo/flagenv"
)

func main() {
	flagenv.Parse()
	flag.Parse()
}
