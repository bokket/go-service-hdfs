# go-service-hdfs 

Hadoop Distributed File System (HDFS) support for [go-storage](https://github.com/beyondstorage/go-storage).

## Notes

**This package has been moved to [go-storage](https://github.com/beyondstorage/go-storage/tree/master/services/hdfs).**

```shell
go get go.beyondstorage.io/services/hdfs
```

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
