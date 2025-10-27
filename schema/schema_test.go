package schema

import (
	"testing"

	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/require"
)

func TestGenerateJSONSchemaSnapshotV1Beta2(t *testing.T) {
	schemaBytes, err := GenerateJSONSchema()
	require.NoError(t, err)

	g := goldie.New(t, goldie.WithFixtureDir("../"), goldie.WithNameSuffix(".json"))
	g.Assert(t, "schema", schemaBytes)
}
