package cluster

import "errors"

var ErrConfigChanged = errors.New("本地配置已变化，请重新选择文件并预览")

// ConfigTransferCodec preserves unmodeled cluster fields during sharing.
type ConfigTransferCodec interface {
	Parse([]byte) (ConfigSnapshot, error)
	Export(ConfigSnapshot) ([]byte, error)
}

type ConfigImportError struct{ Message string }

func (e *ConfigImportError) Error() string { return e.Message }

type ConfigImportConflict struct {
	Name             string `json:"name"`
	BootstrapServers string `json:"bootstrapServers"`
	Reason           string `json:"reason"`
}
type ConfigImportEntry struct {
	Index            int                    `json:"index"`
	Name             string                 `json:"name"`
	BootstrapServers string                 `json:"bootstrapServers"`
	Conflicts        []ConfigImportConflict `json:"conflicts"`
	Overlaps         []int                  `json:"overlaps"`
}
type ConfigImportPreview struct {
	Revision string              `json:"revision"`
	Entries  []ConfigImportEntry `json:"entries"`
}
type ConfigImportResult struct {
	Added    int `json:"added"`
	Replaced int `json:"replaced"`
	Skipped  int `json:"skipped"`
}
