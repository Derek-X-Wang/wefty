package lima

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"gopkg.in/yaml.v3"
)

// UpdateInstanceSizing changes only the sizing that setup owns (cpus, memory,
// disk) in an existing instance's lima.yaml. When Lima creates an instance it
// resolves the template's `base` into `images` and parameters, and it refuses
// to load an instance config that still names a `base` or has no `images`.
// Setup therefore never writes the unresolved template over an instance
// (#1595). A missing instance config is left alone: the instance does not
// exist yet, and creation starts from the setup template.
func UpdateInstanceSizing(path string, sizing Sizing) error {
	if !filepath.IsAbs(path) {
		return errors.New("Lima instance config path must be absolute")
	}
	if err := sizing.Validate(); err != nil {
		return err
	}
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var document yaml.Node
	if err := yaml.Unmarshal(payload, &document); err != nil {
		return fmt.Errorf("read Lima instance config: %w", err)
	}
	if document.Kind != yaml.DocumentNode || len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		return errors.New("Lima instance config is not a YAML mapping")
	}
	root := document.Content[0]
	if mappingValue(root, "base") != nil || mappingValue(root, "images") == nil {
		return errors.New("Lima instance config is not resolved (it names a base or has no images); Lima cannot load it; restore the instance's lima.yaml from Lima's own record before running setup again")
	}
	setMappingScalar(root, "cpus", "!!int", strconv.Itoa(sizing.CPUs), 0)
	setMappingScalar(root, "memory", "!!str", sizing.Memory, yaml.DoubleQuotedStyle)
	setMappingScalar(root, "disk", "!!str", sizing.Disk, yaml.DoubleQuotedStyle)
	var encoded bytes.Buffer
	encoder := yaml.NewEncoder(&encoded)
	encoder.SetIndent(2)
	if err := encoder.Encode(&document); err != nil {
		return fmt.Errorf("encode Lima instance config: %w", err)
	}
	if err := encoder.Close(); err != nil {
		return fmt.Errorf("encode Lima instance config: %w", err)
	}
	return replaceFile(path, encoded.Bytes())
}

func mappingValue(mapping *yaml.Node, key string) *yaml.Node {
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index+1]
		}
	}
	return nil
}

func setMappingScalar(mapping *yaml.Node, key, tag, value string, style yaml.Style) {
	if node := mappingValue(mapping, key); node != nil {
		*node = yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: value, Style: style,
			LineComment: node.LineComment, HeadComment: node.HeadComment, FootComment: node.FootComment}
		return
	}
	mapping.Content = append(mapping.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
		&yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: value, Style: style})
}

// replaceFile writes payload beside path with mode 0600 and renames it into
// place, so a reader sees either the old or the new config, never a partial one.
func replaceFile(path string, payload []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".wefty-lima-config-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}
