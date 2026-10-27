// Package unifiedcontract is an independently versioned extension. It does not
// change or negotiate away the original rc1/media/artifact/interaction contracts.
package unifiedcontract

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	base "everything-go/contracts/task-api/v1"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const Version = "1.0.0-rc.6"

//go:embed api-contract.schema.json
var Schema []byte

func Hash() string { h := sha256.Sum256(Schema); return hex.EncodeToString(h[:]) }
func Validate(raw []byte) error {
	v, e := base.Parse(raw)
	if e != nil {
		return e
	}
	if e = base.ValidateWireNumbers(v); e != nil {
		return e
	}
	return base.Validate2020(Schema, raw)
}
func ToolInput() (map[string]any, error) {
	var v map[string]any
	err := json.Unmarshal(Schema, &v)
	return v, err
}

// ValidateResponse checks the independently closed rc6 DTO at its declared 8MiB
// response bound. Requests keep the original stricter 256KiB duplicate-key parser.
func ValidateResponse(raw []byte) error {
	if len(raw) > 8*1024*1024 {
		return errors.New("response too large")
	}
	var schema map[string]any
	if e := json.Unmarshal(Schema, &schema); e != nil {
		return e
	}
	schema["$ref"] = "#/$defs/Response"
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	if e := compiler.AddResource("urn:bridge:unified:response", schema); e != nil {
		return e
	}
	v, e := compiler.Compile("urn:bridge:unified:response")
	if e != nil {
		return e
	}
	var value any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if e = d.Decode(&value); e != nil {
		return e
	}
	if e = base.ValidateWireNumbers(value); e != nil {
		return e
	}
	return v.Validate(value)
}
