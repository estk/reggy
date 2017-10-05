package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"regexp"
)

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/schemas/{name}/{version}", SchemaHandler)
	http.Handle("/", r)
}

type cacheEntry struct {
	body string
}

type ReggyConfig struct{}

type Reggy struct {
	ReggyConfig
	schemaCache map[string](map[string]cacheEntry)
}

func (r *Reggy) GetSchema(name, version string) (string, error) {
	nOk, _ := checkSchemaName(name)
	vOk, _ := checkSchemaVersion(version)
	if !nOk && !vOk {
		return "", errors.New("Schema name and/or version not valid.")
	}
	vs, hit := r.schemaCache[name]
	if hit {
		v, hit2 := vs[version]
		if hit2 {
			return v.body, nil
		}
	} else {
		r.schemaCache[name] = make(map[string]cacheEntry)
	}
	body, err := r.loadSchema(name, version)
	if err != nil {
		return body, err
	}
	r.schemaCache[name][version] = cacheEntry{body}
	return body, nil
}

func (r Reggy) loadSchema(name, version string) (string, error) {
	fn := fmt.Sprintf("schema/%s/%s.avro", name, version)
	bs, err := ioutil.ReadFile(fn)
	return string(bs), err
}
func checkSchemaVersion(ver string) (bool, error) {
	return regexp.MatchString(`[0-9]+\.[0-9]+\\.[0-9]+`, ver)
}
func checkSchemaName(name string) (bool, error) {
	return regexp.MatchString(`^[a-zA-Z0-9_]*$`, name)
}
