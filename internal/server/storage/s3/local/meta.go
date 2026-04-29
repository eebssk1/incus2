package local

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"time"
)

// metaSuffix is appended to the data filename to form the metadata.
const metaSuffix = ".meta"

// objectMeta is the metadata stored alongside object data files.
type objectMeta struct {
	ContentType string            `json:"content_type,omitempty"`
	ETag        string            `json:"etag"`
	Size        int64             `json:"size"`
	LastMod     time.Time         `json:"last_modified"`
	UserMeta    map[string]string `json:"user_meta,omitempty"`
}

func readMeta(metaPath string) (*objectMeta, error) {
	b, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, err
	}

	m := &objectMeta{}
	err = json.Unmarshal(b, m)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func writeMeta(metaPath string, m *objectMeta) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}

	tmp := metaPath + ".tmp"
	err = os.WriteFile(tmp, b, 0o600)
	if err != nil {
		return err
	}

	return os.Rename(tmp, metaPath)
}

func metaPathFor(dataPath string) string {
	return dataPath + metaSuffix
}

func removeMeta(metaPath string) error {
	err := os.Remove(metaPath)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}

	return nil
}
