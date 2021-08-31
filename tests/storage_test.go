package tests

import (
	"os"
	"testing"

	tests "github.com/beyondstorage/go-integration-test/v4"
)

func TestStorage(t *testing.T) {
	if os.Getenv("STORAGE_HDFS_INTEGRATION_TEST") != "on" {
		t.Skipf("STORAGE_HDFS_INTEGRATION_TEST is not 'on', skipped")
	}
	tests.TestStorager(t, setupTest(t))
}

func TestDirer(t *testing.T) {
	if os.Getenv("STORAGE_HDFS_INTEGRATION_TEST") != "on" {
		t.Skipf("STORAGE_HDFS_INTEGRATION_TEST is not 'on', skipped")
	}
	tests.TestDirer(t, setupTest(t))
}

func TestMover(t *testing.T) {
	if os.Getenv("STORAGE_HDFS_INTEGRATION_TEST") != "on" {
		t.Skipf("STORAGE_HDFS_INTEGRATION_TEST is not 'on', skipped")
	}
	tests.TestMover(t, setupTest(t))
}

func TestAppender(t *testing.T) {
	if os.Getenv("STORAGE_HDFS_INTEGRATION_TEST") != "on" {
		t.Skipf("STORAGE_HDFS_INTEGRATION_TEST is not 'on', skipped")
	}
	tests.TestAppender(t, setupTest(t))
}