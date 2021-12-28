package avro

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/algorand/go-algorand/crypto"
	"github.com/algorand/go-algorand/data/basics"
	"github.com/algorand/go-algorand/data/bookkeeping"
	"github.com/linkedin/goavro/v2"

	"github.com/algorand/go-algorand/data/transactions"
	"github.com/algorand/go-codec/codec"

	log "github.com/sirupsen/logrus"

	"github.com/algorand/go-algorand/protocol"
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
		gsBucket:   "algo-export",
		gProjectId: "algorand-urtho",
	}
	//TODO common cancel context
	ctx, cf := context.WithCancel(context.Background())
	idb.cancel = cf

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

	if ap, err := makeAvroPipe(ctx, "txns", avsc.SchemaTXNs, errChan, idb.gsBucket); err != nil {
		return nil, nil, err
	} else {
		idb.avroPipes.txns = ap
	}

	ch := make(chan struct{})
	close(ch)
	return idb, ch, nil
}

func (db *avroIndexerDb) Close() {
	db.log.Info("Closing indexer")
	db.cancel()
}

func encodeJSON(obj interface{}) []byte {
	var buf []byte
	enc := codec.NewEncoderBytes(&buf, jsonCodecHandle)
	enc.MustEncode(obj)
	return buf
}

var jsonCodecHandle *codec.JsonHandle

func init() {
	jsonCodecHandle = new(codec.JsonHandle)
	jsonCodecHandle.ErrorIfNoField = true
	jsonCodecHandle.ErrorIfNoArrayExpand = true
	jsonCodecHandle.Canonical = true
	jsonCodecHandle.RecursiveEmptyCheck = true
	jsonCodecHandle.HTMLCharsAsIs = true
	jsonCodecHandle.Indent = 0
	jsonCodecHandle.MapKeyAsString = true
}

type blockHeader struct {
	_struct struct{} `codec:",omitempty,omitemptyarray"`
	bookkeeping.BlockHeader
	BranchOverride      string `codec:"prev"`
	FeeSinkOverride     string `codec:"fees"`
	RewardsPoolOverride string `codec:"rwd"`
}

func convertBlockHeader(header bookkeeping.BlockHeader) blockHeader {
	return blockHeader{
		BlockHeader:         header,
		BranchOverride:      bookkeeping.BlockHash(header.Branch).String(),
		FeeSinkOverride:     basics.Address(header.FeeSink).String(),
		RewardsPoolOverride: basics.Address(header.RewardsPool).String(),
	}
}

func avroNullLong(i int64) interface{} {
	if i == 0 {
		return nil
	}
	return goavro.Union("long", i)
}

func avroNullULong(i uint64) interface{} {
	if i == 0 {
		return nil
	}
	return goavro.Union("long", int64(i))
}

func avroNullULong0(i uint64) interface{} {
	return goavro.Union("long", int64(i))
}

func avroNullStr(s string) interface{} {
	if len(s) == 0 {
		return nil
	}
	return goavro.Union("string", s)
}

func avroNullBytes(b []byte) interface{} {
	if b == nil || len(b) == 0 {
		return nil
	}
	return goavro.Union("bytes", b)
}

func encodeBlock(b *blockHeader) map[string]interface{} {
	dat := map[string]interface{}{}

	dat["timestamp"] = time.Unix(b.BlockHeader.TimeStamp, 0)
	dat["earn"] = avroNullULong(b.RewardsLevel)
	dat["fees"] = avroNullStr(b.FeeSinkOverride)
	dat["frac"] = avroNullULong(b.RewardsResidue)
	dat["gen"] = avroNullStr(b.GenesisID)
	dat["gh"] = avroNullStr(b.GenesisHash.String())
	dat["prev"] = avroNullStr(b.BranchOverride)
	dat["proto"] = avroNullStr(string(b.UpgradeState.CurrentProtocol))
	dat["rate"] = avroNullULong(b.RewardsRate)
	dat["rnd"] = avroNullULong0(uint64(b.Round))
	dat["rwcalr"] = avroNullULong(uint64(b.RewardsRecalculationRound))
	dat["rwd"] = avroNullStr(b.RewardsPoolOverride)
	dat["seed"] = avroNullStr(crypto.Digest(b.Seed).String())
	dat["tc"] = avroNullULong0(b.TxnCounter)
	dat["ts"] = avroNullULong(uint64(b.TimeStamp))
	dat["txn"] = avroNullStr(b.TxnRoot.String())
	dat["nextbefore"] = avroNullULong(uint64(b.NextProtocolVoteBefore))
	dat["nextswitch"] = avroNullULong(uint64(b.NextProtocolSwitchOn))
	dat["nextproto"] = avroNullStr(string(b.NextProtocol))
	dat["nextyes"] = avroNullULong(b.NextProtocolApprovals)
	if len(b.UpgradePropose) > 0 {
		dat["upgradeprop"] = avroNullStr(string(b.UpgradePropose))
		dat["upgradeyes"] = goavro.Union("boolean", b.UpgradeApprove)
		dat["upgradedelay"] = avroNullULong0(b.NextProtocolApprovals)
	}
	return dat
}

func encodePayTx(sTxn *transactions.SignedTxnInBlock) map[string]interface{} {
	tx := sTxn.Txn
	dat := map[string]interface{}{}

	dat["amt"] = avroNullULong0(tx.Amount.Raw)
	dat["fee"] = avroNullULong0(tx.Fee.Raw)
	dat["fv"] = avroNullULong(uint64(tx.FirstValid))
	dat["lv"] = avroNullULong(uint64(tx.LastValid))

	if tx.Note != nil {
		dat["note"] = avroNullBytes(tx.Note)
	}
	dat["snd"] = avroNullStr(tx.Sender.GetUserAddress())
	dat["rcv"] = avroNullStr(tx.Receiver.GetUserAddress())
	dat["type"] = avroNullStr(string(tx.Type))

	return dat
}

func (db *avroIndexerDb) AddBlock(block *bookkeeping.Block) error {
	bHeader := convertBlockHeader(block.BlockHeader)
	db.avroPipes.blocks.rowChan <- &rowStruct{native: encodeBlock(&bHeader)}

	for _, sTxn := range block.Payset {
		var dat map[string]interface{}
		txid := sTxn.Txn.ID()

		switch sTxn.Txn.Type {
		case protocol.PaymentTx:
			dat = encodePayTx(&sTxn)
		default:
			//			log.Infof("Ignoring TX of type %s", sTxn.Txn.Type)
			continue
		}
		dat["timestamp"] = time.Unix(bHeader.BlockHeader.TimeStamp, 0)
		dat["rnd"] = avroNullULong(uint64(bHeader.Round))
		dat["id"] = avroNullBytes(txid[:])
		dat["sig"] = avroNullBytes(sTxn.Sig[:])

		db.avroPipes.txns.rowChan <- &rowStruct{native: dat}

	}
	return nil
}

// LoadGenesis is part of idb.IndexerDB
func (db *avroIndexerDb) LoadGenesis(genesis bookkeeping.Genesis) (err error) {
	db.log.Info("LoadGen")
	return nil
}

// GetNextRoundToAccount is part of idb.IndexerDB
func (db *avroIndexerDb) GetNextRoundToAccount() (uint64, error) {
	return 13400000, nil
}

// GetNextRoundToLoad is part of idb.IndexerDB
func (db *avroIndexerDb) GetNextRoundToLoad() (uint64, error) {
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
