package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsReservedField(t *testing.T) {
	require.True(t, IsReservedField("created_at"))
	require.True(t, IsReservedField("updated_at"))
	require.False(t, IsReservedField("id"))
}
