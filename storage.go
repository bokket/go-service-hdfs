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

func (s *Storage) commitAppend(ctx context.Context, o *Object, opt pairStorageCommitAppend) (err error) {
	return
}

func (s *Storage) create(path string, opt pairStorageCreate) (o *Object) {
	rp := s.getAbsPath(path)
	if opt.ObjectMode.IsDir() && opt.HasObjectMode {
		o = s.newObject(false)
		o.Mode = ModeDir
	} else {
		o = s.newObject(false)
		o.Mode = ModeRead
	}

	o.ID = rp
	o.Path = path
	return o
}

func (s *Storage) createAppend(ctx context.Context, path string, opt pairStorageCreateAppend) (o *Object, err error) {
	rp := s.getAbsPath(path)
	dir := filepath.Dir(rp)
	err = s.hdfs.MkdirAll(dir, 0666)
	if err != nil {
		return
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
		return
	}
	defer func() {
		f.Close()
	}()

	o = s.newObject(true)
	o.ID = rp
	o.Path = path
	o.Mode = ModeRead | ModeAppend
	o.SetAppendOffset(0)
	return o, nil
}

func (s *Storage) createDir(ctx context.Context, path string, opt pairStorageCreateDir) (o *Object, err error) {
	rp := s.getAbsPath(path)

	err = s.hdfs.MkdirAll(rp, 0755)
	if err != nil {
		return
	}

	o = s.newObject(true)
	o.ID = rp
	o.Path = path
	o.Mode |= ModeDir
	return o, err
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
	if !opt.HasListMode || opt.ListMode.IsDir() {
		input := &listDirInput{
			rp:                s.getAbsPath(path),
			continuationToken: opt.ContinuationToken,
		}
		return NewObjectIterator(ctx, s.listDirNext, input), nil
	} else {
		return nil, services.ListModeInvalidError{Actual: opt.ListMode}
	}
}

func (s *Storage) metadata(opt pairStorageMetadata) (meta *StorageMeta) {
	meta = NewStorageMeta()
	meta.WorkDir = s.workDir
	return meta
}

func (s *Storage) move(ctx context.Context, src string, dst string, opt pairStorageMove) (err error) {
	rs := s.getAbsPath(src)
	rd := s.getAbsPath(dst)

	stat, err := s.hdfs.Stat(rd)
	if err == nil {
		if stat.IsDir() {
			return services.ErrObjectModeInvalid
		}
	}
	return s.hdfs.Rename(rs, rd)
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt pairStorageRead) (n int64, err error) {
	rp := s.getAbsPath(path)
	f, err := s.hdfs.Open(rp)

	defer func() {
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()

	if err != nil {
		return 0, err
	}
	if opt.HasOffset {
		_, err := f.Seek(opt.Offset, 0)
		if err != nil {
			return 0, err
		}
	}

	var rc io.Reader
	rc = f

	if opt.HasIoCallback {
		rc = iowrap.CallbackReader(rc, opt.IoCallback)
	}
	if opt.HasSize {
		return io.CopyN(w, f, opt.Size)
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
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
	}()

	if opt.HasIoCallback {
		r = iowrap.CallbackReader(r, opt.IoCallback)
	}

	return io.CopyN(f, r, size)
}

func (s *Storage) writeAppend(ctx context.Context, o *Object, r io.Reader, size int64, opt pairStorageWriteAppend) (n int64, err error) {
	f, err := s.hdfs.Append(o.ID)
	if err != nil {
		return
	}
	defer func() {
		f.Close()
	}()

	return io.CopyN(f, r, size)
}
