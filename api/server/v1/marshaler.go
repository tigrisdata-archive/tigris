package api

import (
	jsoniter "github.com/json-iterator/go"
)

// MarshalJSON on read response avoid any encoding/decoding on x.Doc. With this approach we are not doing any extra
// marshaling/unmarshalling in returning the data from the database. The document returned from the database is stored
// in x.Doc and will return as-is.
//
// Note: This also means any changes in ReadResponse proto needs to make sure that we add that here and similarly
// the openAPI specs needs to be specify Doc as object instead of bytes.
func (x *ReadResponse) MarshalJSON() ([]byte, error) {
	b := []byte(`{"doc":`)
	b = append(b, x.Doc...)
	b = append(b, []byte(`}`)...)
	return b, nil
}

// UnmarshalJSON on ReadRequest avoids unmarshalling filter and instead this way we can write a custom struct to do
// the unmarshalling and will be avoiding any extra allocation/copying.
func (x *ReadRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		switch key {
		case "db":
			if err := jsoniter.Unmarshal(value, &x.Db); err != nil {
				return err
			}
		case "collection":
			if err := jsoniter.Unmarshal(value, &x.Collection); err != nil {
				return err
			}
		case "filter":
			// not decoding it here and let it decode during filter parsing
			x.Filter = value
		case "keys":
			if err := jsoniter.Unmarshal(value, &x.Keys); err != nil {
				return err
			}
		case "options":
			if err := jsoniter.Unmarshal(value, &x.Options); err != nil {
				return err
			}
		}
	}
	return nil
}

// UnmarshalJSON on InsertRequest avoids unmarshalling user document. We only need to extract primary/index keys from
// the document and want to store the document as-is in the database. This way there is no exta cost of serialization/deserialization
// and also less error prone because we are not touching the user document.
func (x *InsertRequest) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}
	for key, value := range mp {
		switch key {
		case "db":
			if err := jsoniter.Unmarshal(value, &x.Db); err != nil {
				return err
			}
		case "collection":
			if err := jsoniter.Unmarshal(value, &x.Collection); err != nil {
				return err
			}
		case "documents":
			if err := jsoniter.Unmarshal(value, &x.Documents); err != nil {
				return err
			}
		case "options":
			if err := jsoniter.Unmarshal(value, &x.Options); err != nil {
				return err
			}
		}
	}
	return nil
}

// UnmarshalJSON is a custom unmarshaler implemented for the Document struct. The req handler needs to extract out
// the relevant keys from this document and should pass it as-is to the underlying engine.
func (x *Document) UnmarshalJSON(data []byte) error {
	var mp map[string]jsoniter.RawMessage
	if err := jsoniter.Unmarshal(data, &mp); err != nil {
		return err
	}

	for key, value := range mp {
		switch key {
		case "doc":
			// no need to decode
			x.Doc = value
		case "options":
			if err := jsoniter.Unmarshal(value, &x.Options); err != nil {
				return err
			}
		}
	}
	return nil
}
