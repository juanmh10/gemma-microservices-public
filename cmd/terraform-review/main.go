// terraform-review checks two saved plans; it never invokes Terraform or Google Cloud.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/juanmh10/gemma-microservices/internal/infrareview"
	"os"
)

func main() {
	dir := flag.String("directory", "results/terraform-full", "private saved-plan directory")
	full := flag.Bool("full", false, "require the bounded fresh full-stack profile")
	flag.Parse()
	var plans [2]infrareview.Plan
	hash := sha256.New()
	for i, name := range []string{"foundation", "application"} {
		raw, err := os.ReadFile(*dir + "/" + name + ".json")
		if err != nil {
			fail("saved JSON plan unavailable")
		}
		plans[i], err = infrareview.Parse(raw)
		if err != nil {
			fail(err.Error())
		}
		binary, err := os.ReadFile(*dir + "/" + name + ".tfplan")
		if err != nil {
			fail("saved binary plan unavailable")
		}
		hash.Write([]byte(name + "\x00"))
		hash.Write(raw)
		hash.Write(binary)
	}
	totals, err := infrareview.Review(plans[0], plans[1], *full)
	if err != nil {
		fail(err.Error())
	}
	for i, name := range []string{"foundation", "application"} {
		s := totals[i]
		fmt.Printf("%s: create=%d update=%d delete=%d replace=%d\n", name, s.Create, s.Update, s.Delete, s.Replace)
	}
	fmt.Printf("plan_bundle_sha256=%s\napply=not-executed pipeline=not-executed\n", hex.EncodeToString(hash.Sum(nil)))
}
func fail(s string) { fmt.Fprintln(os.Stderr, s); os.Exit(1) }
