package avro

import (
	log "github.com/sirupsen/logrus"

	"github.com/algorand/indexer/idb"
)

type avroFactory struct {
}

// Name is part of the IndexerFactory interface.
func (df avroFactory) Name() string {
	return "avro"
}

// Build is part of the IndexerFactory interface.
func (df avroFactory) Build(arg string, opts idb.IndexerDbOptions, log *log.Logger) (idb.IndexerDb, chan struct{}, error) {
	return IndexerDb(arg, opts, log)
}

func init() {
	idb.RegisterFactory("avro", &avroFactory{})
}
