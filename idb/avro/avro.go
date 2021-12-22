package avro

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	avsc "github.com/algorand/indexer/idb/avro/avsc"
	"github.com/linkedin/goavro/v2"
	log "github.com/sirupsen/logrus"
)

const (
	packageQueueDepth = 10            // Block incoming packages after buffering this many requests for a single table name
	blockSize         = 100           // Flush to avro file after this many records
	rotateRecords     = 1_000_000     // Rotate avro file after this many records
	rotateAge         = 3600          // Rotate avro file once it is older than this many senconds
	avroWorkFolder    = "output"      // Avro working dir
	avroTmpFolder     = "tmp"         // Avro temp folder with .avro.tmp files - cleared on service start
	avroStageFolder   = "stage"       // Avro staging dir - closed files , queued for upload end up here
	avroErroredFolder = "stage/error" // Staged avro files that have issues with immediate upload end up here (retries are made every minute)
)

type avroPipe struct {
	ctx        context.Context
	codec      *goavro.Codec
	rotateAge  int
	rotateRecs int
	nameBase   string
	workDir    string
	gsBucket   string
	blockSize  int
	rowChan    chan []byte
	errChan    chan error
	buffer     *avroBuffer
}

func makeAvroPipe(ctx context.Context, nameBase string, schema string, errChan chan error, gsBucket string) (*avroPipe, error) {
	rc := make(chan []byte, packageQueueDepth)
	aPipe := &avroPipe{
		ctx:        ctx,
		nameBase:   nameBase,
		rowChan:    rc,
		errChan:    errChan,
		rotateRecs: rotateRecords,
		rotateAge:  rotateAge,
		workDir:    avroWorkFolder,
		gsBucket:   gsBucket,
		blockSize:  blockSize,
	}
	if codec, err := goavro.NewCodec(avsc.SchemaBlocks); err != nil {
		return nil, fmt.Errorf("parsing AVSC schema for %s : %v", nameBase, err)
	} else {
		aPipe.codec = codec
	}

	if err := aPipe.makeAvroBuffer(); err != nil {
		return nil, err
	}

	go aPipe.Run()
	return aPipe, nil
}

func (pipe *avroPipe) Run() {
Forever:
	for {
		select {
		case <-pipe.ctx.Done():
			break Forever
		case jsonRec, ok := <-pipe.rowChan:
			if !ok {
				break Forever
			}
			rotate, err := pipe.buffer.Append(pipe.ctx, jsonRec)
			if err != nil {
				pipe.errChan <- err
				break Forever
			}
			if rotate {
				if e := mustSucceed(pipe.ctx, pipe.buffer.Rotate, time.Minute); e != nil {
					pipe.errChan <- e
					break Forever
				}
				pipe.buffer.Commit(pipe.ctx)
				if e := pipe.makeAvroBuffer(); e != nil {
					pipe.errChan <- e
					break Forever
				}
			}
		}
	}
	log.Warnf("Quitting AVRO pipe for %s", pipe.nameBase)
}

type avroBuffer struct {
	pipe              *avroPipe
	records           []interface{}
	fileName          string
	fileHanle         *os.File
	rotateAge         time.Duration
	flushRecordsAt    int
	unflushedRecords  int
	flushedRecords    int
	tmCreated         time.Time
	uncommitedRecords int
	wrkFolder         string
	gsDstBucket       string
	ocfWriter         *goavro.OCFWriter
}

func (ab *avroBuffer) Commit(ctx context.Context) {
	log.Debugf("Commiting %d records in %s", ab.uncommitedRecords, ab.fileName)
	ab.uncommitedRecords = 0
}

func (ab *avroBuffer) Append(ctx context.Context, jsonRec []byte) (bool, error) {
	native, _, err := ab.pipe.codec.NativeFromTextual(jsonRec)
	if err != nil {
		return false, err
	}
	if ab.records == nil {
		ab.records = make([]interface{}, 0, ab.flushRecordsAt)
	}
	ab.records = append(ab.records, native)
	ab.uncommitedRecords++
	ab.unflushedRecords++
	if time.Since(ab.tmCreated) >= ab.rotateAge {
		log.Infof("Rotating %d records in %s because of age: %ds", ab.uncommitedRecords, ab.fileName, ab.rotateAge/time.Second)
		mustSucceed(ctx, ab.Flush, 10*time.Second)
		return true, nil
	}
	if ab.uncommitedRecords >= rotateRecords {
		log.Infof("Rotating %d records in %s because of row count: %d", ab.uncommitedRecords, ab.fileName, rotateRecords)
		mustSucceed(ctx, ab.Flush, 10*time.Second)
		return true, nil
	}
	if ab.unflushedRecords >= ab.flushRecordsAt {
		//log.Debugf("Flushing %d records in %s because of row count: %d", ab.unflushedRecords, ab.fileName, ab.flushRecordsAt)
		mustSucceed(ctx, ab.Flush, 10*time.Second)
		return false, nil
	}
	return false, nil
}

func (ab *avroBuffer) Flush(ctx context.Context) error {
	var flushed int = 0
	timer := PStart()
	if e := ab.ocfWriter.Append(ab.records); e != nil {
		return e
	}
	ab.flushedRecords += len(ab.records)
	flushed += len(ab.records)
	ab.records = nil
	ab.unflushedRecords = 0
	dur := timer.ElapsedMs()
	timer = PStart()
	//runtime.GC()
	gcDur := timer.ElapsedMs()
	log.WithFields(log.Fields{
		"action": "AvroFlush",
		"msec":   dur + gcDur,
	}).Debugf("Flushing %d records to %s took %dms with %dms GC", flushed, ab.fileName, dur, gcDur)
	return nil
}

func (ab *avroBuffer) Rotate(ctx context.Context) error {
	if e := ab.fileHanle.Close(); e != nil {
		return e
	}
	src := filepath.FromSlash(fmt.Sprintf("%s/%s/%s.avro.tmp", ab.wrkFolder, avroTmpFolder, ab.fileName))
	dst := filepath.FromSlash(fmt.Sprintf("%s/%s/%s.avro", ab.wrkFolder, avroStageFolder, ab.fileName))
	if e := os.Rename(src, dst); e != nil {
		return e
	}
	if ab.gsDstBucket != "" {
		GSSaveFileBG(ab.gsDstBucket, dst, ab.fileName+".avro", true)
	}
	return nil
}

func (pipe *avroPipe) makeAvroBuffer() error {
	tn := time.Now().UTC()
	ab := &avroBuffer{
		records:           make([]interface{}, 0, blockSize),
		uncommitedRecords: 0,
		fileName:          fmt.Sprintf("%s-%sNT%d", pipe.nameBase, tn.Format("D20060102T150405"), tn.Nanosecond()),
		rotateAge:         time.Second * time.Duration(pipe.rotateAge),
		flushRecordsAt:    pipe.blockSize,
		tmCreated:         time.Now(),
		wrkFolder:         pipe.workDir,
		gsDstBucket:       pipe.gsBucket,
		pipe:              pipe,
	}
	var e error
	fName := filepath.FromSlash(fmt.Sprintf("%s/%s/%s.avro.tmp", ab.wrkFolder, avroTmpFolder, ab.fileName))
	if ab.fileHanle, e = os.Create(fName); e != nil {
		return e
	}
	log.Debugf("Created new avro file %s", fName)
	if ab.ocfWriter, e = goavro.NewOCFWriter(goavro.OCFConfig{
		W:               ab.fileHanle,
		Codec:           pipe.codec,
		CompressionName: goavro.CompressionDeflateLabel,
	}); e != nil {
		return e
	}
	pipe.buffer = ab
	return nil
}

func cleanupTmp(tmpDir string) error {
	d, err := os.Open(tmpDir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	log.Debugf("Cleaning up tmp dir %s", tmpDir)
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(tmpDir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

func mustSucceed(ctx context.Context, ff func(ctx context.Context) error, wait time.Duration) error {
	for {
		e := ff(ctx)
		if e == nil {
			return nil
		}
		log.Error(e)
		select {
		case <-ctx.Done():
			return context.Canceled
		case <-time.After(wait):
		}
	}
}
