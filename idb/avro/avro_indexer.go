package avro

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/algorand/go-algorand/data/transactions"
	log "github.com/sirupsen/logrus"

	models "github.com/algorand/indexer/api/generated/v2"
	"github.com/algorand/indexer/idb"
	"github.com/algorand/indexer/idb/avro/avsc"
)

type avroIndexerDb struct {
	log        *log.Logger
	cancel     context.CancelFunc
	gsBucket   string
	gProjectId string
	avroPipes  struct {
		blocks *avroPipe
		txns   *avroPipe
	}
}

func IndexerDb(connection string, opts idb.IndexerDbOptions, log *log.Logger) (*avroIndexerDb, chan struct{}, error) {
	idb := &avroIndexerDb{
		log:        log,
		gsBucket:   "algo-export/stage",
		gProjectId: "algorand-urtho",
	}
	//TODO common cancel context
	ctx, cf := context.WithCancel(context.Background())
	idb.cancel = cf
	defer cf()

	{
		cancelCh := make(chan os.Signal, 1)
		signal.Notify(cancelCh, syscall.SIGTERM, syscall.SIGINT)
		go func() {
			<-cancelCh
			log.Println("Stopping AVRO pipes.")
			cf()
		}()
	}

	errChan := make(chan error, 100)

	go func() {
		err := <-errChan
		cf()
		log.Fatal(err)
	}()

	stageDir := filepath.FromSlash(fmt.Sprintf("%s/%s", avroWorkFolder, avroStageFolder))
	errorDir := filepath.FromSlash(fmt.Sprintf("%s/%s", avroWorkFolder, avroErroredFolder))
	tmpDir := filepath.FromSlash(fmt.Sprintf("%s/%s", avroWorkFolder, avroTmpFolder))
	if err := os.MkdirAll(stageDir, 0700); err != nil {
		return nil, nil, fmt.Errorf("creating stageDir: %v", err)
	}
	if err := os.MkdirAll(errorDir, 0700); err != nil {
		return nil, nil, fmt.Errorf("creating errorDir: %v", err)
	}
	if err := os.MkdirAll(tmpDir, 0700); err != nil {
		return nil, nil, fmt.Errorf("creating tmpDir: %v", err)
	}
	if err := cleanupTmp(tmpDir); err != nil {
		return nil, nil, fmt.Errorf("cleaning up tmpDir: %v", err)
	}
	if len(os.Getenv("NOGS")) == 0 && len(idb.gsBucket) > 0 {
		if e := GSSetup(ctx, idb.gProjectId, idb.gsBucket, stageDir, errorDir, 200, 20); e != nil {
			return nil, nil, e
		}
	} else {
		log.Warnf("Google Storage upload disabled!")
	}

	if ap, err := makeAvroPipe(ctx, "blocks", avsc.SchemaBlocks, errChan, idb.gsBucket); err != nil {
		return nil, nil, err
	} else {
		idb.avroPipes.blocks = ap
	}

	return idb, nil, nil
}

func (db *avroIndexerDb) Close() {
	db.log.Info("Closing indexer")
	db.cancel()
}

func (db *avroIndexerDb) AddBlock(block *bookkeeping.Block) error {
	db.log.Printf("AddBlock %v", block.BlockHeader.Round)
	return nil
}

// LoadGenesis is part of idb.IndexerDB
func (db *avroIndexerDb) LoadGenesis(genesis bookkeeping.Genesis) (err error) {
	db.log.Info("LoadGen")
	return nil
}

// GetNextRoundToAccount is part of idb.IndexerDB
func (db *avroIndexerDb) GetNextRoundToAccount() (uint64, error) {
	db.log.Info("GNR")
	return 0, nil
}

// GetNextRoundToLoad is part of idb.IndexerDB
func (db *avroIndexerDb) GetNextRoundToLoad() (uint64, error) {
	db.log.Info("GNRtL")
	return 0, nil
}

// GetSpecialAccounts is part of idb.IndexerDb
func (db *avroIndexerDb) GetSpecialAccounts() (transactions.SpecialAddresses, error) {
	return transactions.SpecialAddresses{}, nil
}

// GetBlock is part of idb.IndexerDB
func (db *avroIndexerDb) GetBlock(ctx context.Context, round uint64, options idb.GetBlockOptions) (blockHeader bookkeeping.BlockHeader, transactions []idb.TxnRow, err error) {
	return bookkeeping.BlockHeader{}, nil, nil
}

// Transactions is part of idb.IndexerDB
func (db *avroIndexerDb) Transactions(ctx context.Context, tf idb.TransactionFilter) (<-chan idb.TxnRow, uint64) {
	return nil, 0
}

// GetAccounts is part of idb.IndexerDB
func (db *avroIndexerDb) GetAccounts(ctx context.Context, opts idb.AccountQueryOptions) (<-chan idb.AccountRow, uint64) {
	return nil, 0
}

// Assets is part of idb.IndexerDB
func (db *avroIndexerDb) Assets(ctx context.Context, filter idb.AssetsQuery) (<-chan idb.AssetRow, uint64) {
	return nil, 0
}

// AssetBalances is part of idb.IndexerDB
func (db *avroIndexerDb) AssetBalances(ctx context.Context, abq idb.AssetBalanceQuery) (<-chan idb.AssetBalanceRow, uint64) {
	return nil, 0
}

// Applications is part of idb.IndexerDB
func (db *avroIndexerDb) Applications(ctx context.Context, filter *models.SearchForApplicationsParams) (<-chan idb.ApplicationRow, uint64) {
	return nil, 0
}

// Health is part of idb.IndexerDB
func (db *avroIndexerDb) Health() (state idb.Health, err error) {
	return idb.Health{}, nil
}
