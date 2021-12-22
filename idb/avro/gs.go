package avro

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	gs "cloud.google.com/go/storage"
	"github.com/korovkin/limiter"
	log "github.com/sirupsen/logrus"
)

type gsJob struct {
	src    string
	dst    string
	bucket string
	done   bool
	move   bool
}

var (
	gsJobChan    chan *gsJob
	gsFileLock   map[string]bool
	gsFileLockMx sync.Mutex
	gsClient     *gs.Client
	gsErrorDir   string
)

//GSSaveFileBG - queues file to be saved
func GSSaveFileBG(bucket, src, dst string, move bool) {
	gsJobChan <- &gsJob{
		bucket: bucket,
		src:    src,
		dst:    dst,
		done:   false,
		move:   move,
	}
}

func gsErroredScanner(ctx context.Context, bucket, errorDir string) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Minute):
			gsEnqueueFiles(ctx, bucket, errorDir)
		}
	}
}

func failJob(job *gsJob) {
	log.Errorf("Failing file %s", job.src)
	dst := filepath.FromSlash(fmt.Sprintf("%s/%s", gsErrorDir, filepath.Base(job.src)))
	if e := os.Rename(job.src, dst); e != nil {
		log.Errorf("!!Error marking file %d for later upload!!, %v", dst, e)
	}
}

func gsBgJob(gsCtx context.Context, job *gsJob) {
	defer func() {
		if !job.done {
			failJob(job)
		}
	}()

	if !gsFileTryLock(job.src) {
		//file is already being taken care of
		job.done = true
		log.Warnf("File %s is already being taken care of", job.src)
		return
	}

	defer gsFileUnlock(job.src)
	log.Debugf("Processing GS job %+v", job)

	var err error

	reader, err := os.Open(job.src)
	if err != nil {
		log.Errorf("Unable to open file %s", job.src)
		return
	}
	//job.dst = time.Now().Format("20060102") + "/" + job.dst
	job.dst = "queue" + "/" + job.dst
	deadCtx, cancel := context.WithTimeout(gsCtx, time.Second*50)
	defer cancel()
	writer := gsClient.Bucket(job.bucket).Object(job.dst).NewWriter(deadCtx)

	defer reader.Close()

	var bytes int64
	if bytes, err = io.Copy(writer, reader); err != nil {
		log.Errorf("createFile: unable to write to bucket %q, file %q: %v", job.bucket, job.dst, err)
		return
	}

	if err = writer.Close(); err != nil {
		log.Errorf("createFile: unable to close bucket %q, file %q: %v", job.bucket, job.dst, err)
		return
	}

	job.done = true
	if job.move {
		os.Remove(job.src)
	}
	log.Infof("File %s, %dKB saved to bucket %s", job.dst, bytes/1024, job.bucket)
}

func gsJobServer(ctx context.Context, workDir string, maxUploads int) {
	lmt := limiter.NewConcurrencyLimiter(maxUploads)
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-gsJobChan:
			lmt.Execute(func() {
				gsBgJob(ctx, job)
			})

		}
	}
}

func gsFileTryLock(file string) bool {
	gsFileLockMx.Lock()
	defer gsFileLockMx.Unlock()
	if locked := gsFileLock[file]; locked {
		return false
	}
	gsFileLock[file] = true
	return true
}

func gsFileUnlock(file string) {
	gsFileLockMx.Lock()
	defer gsFileLockMx.Unlock()
	delete(gsFileLock, file)
}

func gsEnqueueFiles(ctx context.Context, bucket, dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	log.Debugf("Bulk scheduling of %d files from %s", len(names), dir)
	for _, name := range names {
		if filepath.Ext(strings.TrimSpace(name)) == ".avro" {
			GSSaveFileBG(bucket, filepath.FromSlash(fmt.Sprintf("%s/%s", dir, name)), name, true)
			select {
			case <-ctx.Done():
				return context.Canceled
			case <-time.After(time.Millisecond * 5):
			}
		}
	}
	return nil
}

//GSSetup - setup the Google Storage integration
func GSSetup(ctx context.Context, projectID, bucket, workDir, errorDir string, jobBuffer, maxUploads int) error {
	gsJobChan = make(chan *gsJob, jobBuffer)
	gsFileLock = make(map[string]bool)
	var e error
	if gsClient, e = gs.NewClient(ctx); e != nil {
		log.Error(e)
		return e
	}
	gsErrorDir = errorDir
	go gsJobServer(ctx, workDir, maxUploads)
	go gsErroredScanner(ctx, bucket, errorDir)
	go gsEnqueueFiles(ctx, bucket, workDir)
	return nil
}
