package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"cloud.google.com/go/storage"
)

func main() {
	fmt.Println("The time is", time.Now())

	//projectID := "pow-play"
	bucketName := "pow-play-cms"
	objectName := "g5-clw-1k22wieece-el-dorado/g5-clw-1k22wieece-el-dorado-01b1ffa18c97fcc815490ad12303891e.tar.gz"

	ctx := context.Background()
	client, err := storage.NewClient(ctx)

	if err != nil {
		log.Fatal(err)
	}

	//data, err := read(client, bucket, object)
	bkt := client.Bucket(bucketName)
	attrs, err := bkt.Attrs(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("bucket %s, created at %s, is located in %s with storage class %s\n",
		attrs.Name, attrs.Created, attrs.Location, attrs.StorageClass)

	obj := bkt.Object(objectName)

	objAttrs, err := obj.Attrs(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("object %s has size %d and can be read using %s\n",
		objAttrs.Name, objAttrs.Size, objAttrs.MediaLink)

	reader, err := obj.NewReader(ctx)
	if err != nil {
		log.Fatal(err)
	}

	readerAttrs := reader.Attrs
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("size %s, startoffset %s, contenttype %s contentencoding %s\n",
		readerAttrs.Size, readerAttrs.StartOffset, readerAttrs.ContentType, readerAttrs.ContentEncoding)

	defer reader.Close()

	file, err := os.Create("tmp/" + "g5-clw-1k22wieece-el-dorado-01b1ffa18c97fcc815490ad12303891e.tar.gz")
	if err != nil {
		log.Fatal(err)
	}

	writer := bufio.NewWriter(file)

	if _, err := io.Copy(writer, reader); err != nil {
		log.Fatal(err)
	}
}