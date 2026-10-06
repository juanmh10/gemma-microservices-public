package analysis

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/google/jsonschema-go/jsonschema"
)

func assertSchema(t *testing.T, name string, payload []byte) {
	t.Helper()
	data, err := os.ReadFile("../../../schemas/analysis/" + name + "-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var schema jsonschema.Schema
	if err = json.Unmarshal(data, &schema); err != nil {
		t.Fatal(err)
	}
	resolved, err := schema.Resolve(nil)
	if err != nil {
		t.Fatal(err)
	}
	var value any
	if err = json.Unmarshal(payload, &value); err != nil {
		t.Fatal(err)
	}
	if err = resolved.Validate(value); err != nil {
		t.Fatal(err)
	}
}
func TestVersionedContractsValidateProducedArtifacts(t *testing.T) {
	r := fixture(t)
	assertSchema(t, "request", JSON(r))
	store := &Objects{}
	_, err := Run(context.Background(), store, r, &narrator{}, Config{PromptSHA256: Digest([]byte("test")), ModelMode: "fake"})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"metrics", "report"} {
		data, err := store.Read(context.Background(), objectPath(r, name+".json"), MaxObjectBytes)
		if err != nil {
			t.Fatal(err)
		}
		assertSchema(t, name, data)
	}
}
