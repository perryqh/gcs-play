package main

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

func main() {
	downloader := GcsDownloader{tmpPath: os.TempDir()}
	downloader.Download("gs://pow-play-cms/g5-clw-1k22xhiz1c-esteban/g5-clw-1k22xhiz1c-esteban-fb887d6647879a11bad83757db35516c.tar.gz")
}

const (
	unexpectedArchiveResponseErrorMessage = "unexpected response for archive download"
	generalDownloadError                  = "unexpected error downloading"
)

// This means that if we deploy, it's going to cut the deploy off. But lots of
// downloads take a while to finish because of relatively slow EBS performance.
// The way we've implemented this pipline means an interrupted download is OK
// and will be picked up next time. It would be better to interrupt with a
// context, but that'll be an overhaul for another day.
var clientWithTimeout = &http.Client{
	Timeout: 5 * time.Minute,
}

// Downloader is an interface for downloading a file and returning a reader of
// its contents, or an error if there were any issues fetching it.
type Downloader interface {
	Download(uri string) (io.ReadCloser, error)
}

// BasicAuthedDownloader implements the Downloader interface and will use basic
// authentication when fetching a file.
type GcsDownloader struct {
	tmpPath string
}

// Download is an interface that sort of just exists to satisfy the mocking
// library. I could return this type, but I don't need to, so I'm stubbornly
// keeping this in case I can ever revisit it:
// https://github.com/matryer/moq/issues/28
type Download interface {
	io.ReadCloser
}

// SelfCleaningDownload implements the io.ReadCloser interface and will clean
// up the file after it is finished.
type SelfCleaningDownload struct {
	file *os.File
}

// Read satisfies io.Reader and delegates to the underlying file.
func (d SelfCleaningDownload) Read(p []byte) (n int, err error) {
	return d.file.Read(p)
}

// Close satisfies io.Closer, closing the file and also deleting it. It logs
// and swallows file closing errors in favor of doing the most important
// action, which is deleting the file. If the file doesn't actually exist, we
// just whistle past that graveyard for reasons enumerated in the comment.
func (d SelfCleaningDownload) Close() error {
	path := d.file.Name()
	_ = d.file.Close()
	// we're pretty diligent about closing in all cases, and some of those cases
	// include the file not existing. Error here end up masking the real problems
	// that caused this to be missing.
	if _, err := os.Stat(path); err != nil && os.IsNotExist(err) {
		return nil
	}
	return os.Remove(path)
}

// IsUnexpectedArchiveResponse returns true if the passed-in error was only due
// to the archive being missing and Bitbucket returned a 404.
func IsUnexpectedArchiveResponse(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), unexpectedArchiveResponseErrorMessage)
}

// IsGeneralDownloadError returns true if the passed-in error was only due
// to an issue downloading an archive.
func IsGeneralDownloadError(err error) bool {
	if err == nil {
		return false
	}
	return strings.HasPrefix(err.Error(), generalDownloadError)
}

type GcsInfo struct {
	uri              string
	bucketName       string
	objectName       string
	archivedFileName string
}

func NewGcsInfo(uri string) *GcsInfo {
	bucketNameRe := regexp.MustCompile(`^gs://([^/]+)`)
	bucketNameMatch := bucketNameRe.FindSubmatch([]byte(uri))
	bucketName := string(bucketNameMatch[1])

	objectNameReString := "^gs://" + bucketName + "/(.+)"
	objectNameRe := regexp.MustCompile(objectNameReString)
	objectNameMatch := objectNameRe.FindSubmatch([]byte(uri))
	objectName := string(objectNameMatch[1])

	splits := strings.Split(objectName, "/")
	archivedFileName := splits[len(splits)-1]
	return &GcsInfo{uri: uri, bucketName: bucketName, objectName: objectName, archivedFileName: archivedFileName}
}

func NewGcsReader(gcsInfo *GcsInfo) (*storage.Reader, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	bkt := client.Bucket(gcsInfo.bucketName)
	attrs, err := bkt.Attrs(ctx)
	if err != nil {
		return nil, err
	}
	fmt.Printf("bucket %s, created at %s, is located in %s with storage class %s\n",
		attrs.Name, attrs.Created, attrs.Location, attrs.StorageClass)

	obj := bkt.Object(gcsInfo.objectName)
	objAttrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, err
	}
	fmt.Printf("object %s has size %d and can be read using %s\n",
		objAttrs.Name, objAttrs.Size, objAttrs.MediaLink)

	reader, err := obj.NewReader(ctx)
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// Download downloads a file a file for the passed-in URL using the basic auth
// credentials that the instance was instantiated with. It will load the entire
// file into memory before passing along the reader, so ensure that delayed
// processing of the contents don't cause the connection to be interrupted.
func (d GcsDownloader) Download(url string) (io.ReadCloser, error) {
	//logrus.WithField("url", url).Debug("downloading")
	//timer := prometheus.NewTimer(processingStepSummaries.WithLabelValues("downloader"))
	//defer timer.ObserveDuration()

	gcsInfo := NewGcsInfo(url)
	reader, err := NewGcsReader(gcsInfo)
	if err != nil {
		return nil, err
	}

	if err != nil {
		//downloadResponseCodeCounter.WithLabelValues("error").Inc()
		return nil, fmt.Errorf(generalDownloadError+": %v", err)
	}
	defer func() {
		//maybeLogrus("closing archive reader", reader.Close())
	}()

	tmpFilePath := filepath.Join(d.tmpPath, gcsInfo.archivedFileName)
	fmt.Println(tmpFilePath)
	// You could read the content of the file into memory, but then memory usage
	// of this service gets pretty unpredictable. This is a tradeoff of speed for
	// reliability.
	archive, err := os.Create(tmpFilePath)
	if err != nil {
		return nil, fmt.Errorf("creating archive tempfile: %v", err)
	}

	// problems from this point on should at least try and clean up the file after themselves
	cleanup := func() {
		// this doesn't cover every single base of what might be going on, but
		// we're just trying to clean up if necessary and get out of here. If we
		// got here, it's because there is already an error that will be reported.
		if _, err := os.Stat(tmpFilePath); err == nil {
			//maybeLogrus("cleaning up failed archive", os.Remove(tmpFilePath))
		}
	}
	n, err := io.Copy(archive, reader)
	fmt.Printf("copied %f", n)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("writing download to archive tempfile: %v", err)
	}
	//downloadedBytesSummary.Observe(float64(n))
	if _, err := archive.Seek(0, 0); err != nil {
		cleanup()
		return nil, fmt.Errorf("rewinding archive tempfile for read: %v", err)
	}

	return SelfCleaningDownload{file: archive}, nil
}
