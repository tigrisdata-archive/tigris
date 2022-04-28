package workload

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Document struct {
	F1 int64
	F2 string
	F3 []byte
	F4 uuid.UUID
	F5 time.Time
}

func NewDocument(uniqueId int64) *Document {
	return &Document{
		F1: uniqueId,
		F2: fmt.Sprintf("id_%d", uniqueId),
		F3: []byte(`1234`),
		F4: uuid.New(),
		F5: time.Now().UTC(),
	}
}

func Serialize(doc *Document) ([]byte, error) {
	return json.Marshal(doc)
}

func Deserialize(raw []byte) (*Document, error) {
	var doc Document
	err := json.Unmarshal(raw, &doc)
	return &doc, err
}
