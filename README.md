[![Build Status](https://github.com/beyondstorage/go-service-hdfs/workflows/Unit%20Test/badge.svg?branch=master)](https://github.com/beyondstorage/go-service-hdfs/actions?query=workflow%3A%22Unit+Test%22)
[![License](https://img.shields.io/badge/license-apache%20v2-blue.svg)](https://github.com/Xuanwo/storage/blob/master/LICENSE)
[![](https://img.shields.io/matrix/beyondstorage@go-storage:matrix.org.svg?logo=matrix)](https://matrix.to/#/#beyondstorage@go-storage:matrix.org)

# go-service-hdfs 

Hadoop Distributed File System (HDFS) support for [go-storage](https://github.com/beyondstorage/go-storage).

## Install

```go
go get github.com/beyondstorage/go-service-hdfs
```

## Usage

```go
import (
	"log"
	
    _ "github.com/beyondstorage/go-service-hdfs"
	"github.com/beyondstorage/go-storage/v4/services"
)

func main() {
	store, err := services.NewStoragerFromString("hdfs:///path/to/workdir?endpoint=tcp:<host>:<port>")
	if err != nil {
		log.Fatal(err)
	}
	
	// Write data from io.Reader into hello.txt
	n, err := store.Write("hello.txt", r, length)
}
```

- See more examples in [go-storage-example](https://github.com/beyondstorage/go-storage-example).
- Read [more docs](https://beyondstorage.io/docs/go-storage/services/hdfs) about go-service-hdfs.