package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"regexp"

	mux "github.com/gorilla/mux"
)

func main() {
	reg := New()
	log.Fatal(http.ListenAndServe(":8000", reg.router))
}

type cacheEntry struct {
	body string
}

type ReggyConfig struct {
	schemaDir string
	schemaExt string
}

func NewReggyConfig() ReggyConfig {
	return ReggyConfig{
		"schemas",
		"avsc",
	}
}

type Reggy struct {
	ReggyConfig
	router      *mux.Router
	schemaCache map[string](map[string]cacheEntry)
}

func (r *Reggy) configureRouter() {
	r.router.HandleFunc("/schemas/{name}/{version}", r.SchemaHandler)
	http.Handle("/", r.router)
}

func (r *Reggy) SchemaHandler(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	name := vars["name"]
	version := vars["version"]
	schema, err := r.GetSchema(name, version)
	if err != nil {
		log.Println(err)
		return
	}

	w.Write([]byte(schema))
	log.Printf("Served schema: %s, version: %s", name, version)
}

func New() *Reggy {
	reg := &Reggy{
		NewReggyConfig(),
		mux.NewRouter(),
		make(map[string](map[string]cacheEntry)),
	}
	reg.configureRouter()
	return reg
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
	fn := fmt.Sprintf("%s/%s/%s.%s", r.schemaDir, name, version, r.schemaExt)
	bs, err := ioutil.ReadFile(fn)
	return string(bs), err
}

func checkSchemaVersion(ver string) (bool, error) {
	return regexp.MatchString(`[0-9]+\.[0-9]+\\.[0-9]+`, ver)
}

func checkSchemaName(name string) (bool, error) {
	return regexp.MatchString(`^[a-zA-Z0-9_]*$`, name)
}
