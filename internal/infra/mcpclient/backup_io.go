package mcpclient

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
)

func createPrivateRootFile(
	root *os.Root,
	prefix string,
	suffix string,
) (string, *os.File, error) {
	for range 32 {
		name, err := privateRootName(root, prefix, suffix)
		if err != nil {
			return "", nil, err
		}
		file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			return name, file, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return "", nil, err
		}
	}
	return "", nil, ErrRollbackFailed
}

func privateRootName(root *os.Root, prefix, suffix string) (string, error) {
	for range 32 {
		random := make([]byte, 16)
		if _, err := rand.Read(random); err != nil {
			return "", ErrRollbackFailed
		}
		name := prefix + hex.EncodeToString(random) + suffix
		if _, err := root.Lstat(name); errors.Is(err, os.ErrNotExist) {
			return name, nil
		}
	}
	return "", ErrRollbackFailed
}

func digestPathContext(ctx context.Context, path string) (FileDigest, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return FileDigest{}, nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return FileDigest{}, ErrRollbackFailed
	}
	file, err := os.Open(path)
	if err != nil {
		return FileDigest{}, ErrRollbackFailed
	}
	defer func() { _ = file.Close() }()
	return digestReader(ctx, file)
}

func digestRootContext(
	ctx context.Context,
	root *os.Root,
	name string,
) (FileDigest, os.FileInfo, error) {
	info, err := root.Lstat(name)
	if errors.Is(err, os.ErrNotExist) {
		return FileDigest{}, nil, nil
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return FileDigest{}, nil, ErrRollbackFailed
	}
	file, err := root.Open(name)
	if err != nil {
		return FileDigest{}, nil, ErrRollbackFailed
	}
	defer func() { _ = file.Close() }()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return FileDigest{}, nil, ErrConcurrentModification
	}
	digest, err := digestReader(ctx, file)
	return digest, opened, err
}

func digestReader(ctx context.Context, reader io.Reader) (FileDigest, error) {
	hash := sha256.New()
	if err := copyBounded(ctx, hash, reader, maxClientConfigBytes); err != nil {
		return FileDigest{}, err
	}
	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	return FileDigest{Exists: true, SHA256: digest}, nil
}

func readRootBounded(
	ctx context.Context,
	root *os.Root,
	name string,
	max int64,
) ([]byte, error) {
	file, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	reader := io.LimitReader(file, max+1)
	raw, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > max || ctx.Err() != nil {
		return nil, contextOrRollback(ctx)
	}
	return raw, nil
}

func copyBounded(
	ctx context.Context,
	dst io.Writer,
	src io.Reader,
	max int64,
) error {
	if ctx == nil {
		return ErrRollbackFailed
	}
	buffer := make([]byte, 32<<10)
	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		count, readErr := src.Read(buffer)
		if count > 0 {
			if total > max-int64(count) {
				return ErrRollbackFailed
			}
			written, writeErr := dst.Write(buffer[:count])
			if writeErr != nil || written != count {
				return ErrRollbackFailed
			}
			total += int64(count)
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return ErrRollbackFailed
		}
	}
}

func contextOrRollback(ctx context.Context) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrRollbackFailed
}

type byteSliceReader struct {
	value []byte
}

func bytesReader(value []byte) *byteSliceReader {
	return &byteSliceReader{value: value}
}

func (reader *byteSliceReader) Read(output []byte) (int, error) {
	if len(reader.value) == 0 {
		return 0, io.EOF
	}
	count := copy(output, reader.value)
	reader.value = reader.value[count:]
	return count, nil
}
