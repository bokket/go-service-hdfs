package hdfs

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/beyondstorage/go-storage/v4/pkg/iowrap"
	"github.com/beyondstorage/go-storage/v4/services"
	. "github.com/beyondstorage/go-storage/v4/types"
)

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	rp := s.getAbsPath(path)
	o.ID = rp
	o.Path = path
	return o
}

func (s *Storage) delete(ctx context.Context, path string, opt pairStorageDelete) (err error) {
	rp := s.getAbsPath(path)
	err = s.hdfs.Remove(rp)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		// Omit `file not exist` error here
		// ref: [GSP-46](https://github.com/beyondstorage/specs/blob/master/rfcs/46-idempotent-delete.md)
		err = nil
	}
	return err
}

func (s *Storage) list(ctx context.Context, path string, opt pairStorageList) (oi *ObjectIterator, err error) {
	rp := s.getAbsPath(path)
	if !opt.HasListMode || opt.ListMode.IsDir() {
		nextFn := func(ctx context.Context, page *ObjectPage) error {
			dir, err := s.hdfs.ReadDir(rp)
			if err != nil {
				return err
			}
			for _, f := range dir {
				o := NewObject(s, true)
				o.Path = f.Name()
				if f.IsDir() {
					o.Mode |= ModeDir
				} else {
					o.Mode |= ModeRead
				}

				o.SetContentLength(f.Size())
				page.Data = append(page.Data, o)
			}
			return IterateDone
		}
		oi = NewObjectIterator(ctx, nextFn, nil)
		return
	} else {
		return nil, services.ListModeInvalidError{Actual: opt.ListMode}
	}
}

func (s *Storage) metadata(opt pairStorageMetadata) (meta *StorageMeta) {
	meta = NewStorageMeta()
	meta.WorkDir = s.workDir
	return meta
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	rp := s.getAbsPath(path)
	f, err := s.hdfs.Open(rp)
	if err != nil {
		return 0, err
	}
	if opt.HasOffset {
		_, err = f.Seek(opt.Offset, 0)
		if err != nil {
			return 0, err
		}
	}

	var rc io.Reader
	rc = f

	if opt.HasIoCallback {
		rc = iowrap.CallbackReader(rc, opt.IoCallback)
	}

	return io.Copy(w, f)
}

func (s *Storage) stat(ctx context.Context, path string, opt pairStorageStat) (o *Object, err error) {
	rp := s.getAbsPath(path)

	stat, err := s.hdfs.Stat(rp)
	if err != nil {
		return nil, err
	}

	o = s.newObject(true)
	o.ID = rp
	o.Path = path

	if stat.IsDir() {
		o.Mode |= ModeDir
		return
	}

	if stat.Mode().IsRegular() {
		o.Mode |= ModeRead
		o.SetContentLength(stat.Size())
		o.SetLastModified(stat.ModTime())
	}

	return o, nil
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt pairStorageWrite) (n int64, err error) {
	rp := s.getAbsPath(path)
	dir := filepath.Dir(rp)
	err = s.hdfs.MkdirAll(dir, 0666)
	if err != nil {
		return 0, err
	}
	_, err = s.hdfs.Stat(rp)
	if err == nil {
		err = s.hdfs.Remove(rp)
		if err != nil && errors.Is(err, os.ErrNotExist) {
			// Omit `file not exist` error here
			// ref: [GSP-46](https://github.com/beyondstorage/specs/blob/master/rfcs/46-idempotent-delete.md)
			err = nil
		}
	}

	f, err := s.hdfs.Create(rp)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = f.Close()
	}()

	if opt.HasIoCallback {
		r = iowrap.CallbackReader(r, opt.IoCallback)
	}

	return io.CopyN(f, r, size)
}
