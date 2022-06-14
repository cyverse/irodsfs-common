package vpath

import (
	"fmt"

	"github.com/cyverse/irodsfs-common/utils"
)

// VPathMappingResourceType determines the type of Virtual Path Mapping resource entry
type VPathMappingResourceType string

const (
	// VPathMappingFile is for file entry
	VPathMappingFile VPathMappingResourceType = "file"
	// VPathMappingDirectory is for directory entry
	VPathMappingDirectory VPathMappingResourceType = "dir"
)

// VPathMapping defines a path mapping between iRODS DataObject/Collection and local file/directory
type VPathMapping struct {
	IRODSPath           string                   `yaml:"irods_path" json:"irods_path"`
	MappingPath         string                   `yaml:"mapping_path" json:"mapping_path"`
	ResourceType        VPathMappingResourceType `yaml:"resource_type" json:"resource_type"`
	ReadOnly            bool                     `yaml:"read_only" json:"read_only"`
	CreateDir           bool                     `yaml:"create_dir" json:"create_dir"`
	IgnoreNotExistError bool                     `yaml:"ignore_not_exist_error" json:"ignore_not_exist_error"`
}

// Validate validates VPathMapping
func (mapping *VPathMapping) Validate() error {
	if !utils.IsAbsolutePath(mapping.IRODSPath) {
		return fmt.Errorf("IRODSPath given (%s) is not absolute path", mapping.IRODSPath)
	}

	if !utils.IsAbsolutePath(mapping.MappingPath) {
		return fmt.Errorf("MappingPath given (%s) is not absolute path", mapping.MappingPath)
	}

	return nil
}

// ValidateVPathMappings validates the path mappings given
func ValidateVPathMappings(mappings []VPathMapping) error {
	mappingDict := map[string]string{}

	for _, mapping := range mappings {
		err := mapping.Validate()
		if err != nil {
			return err
		}

		// check mapping path is used in another mapping
		if _, ok := mappingDict[mapping.MappingPath]; ok {
			// exists
			return fmt.Errorf("MappingPath given (%s) is already used in another mapping", mapping.MappingPath)
		}

		mappingDict[mapping.MappingPath] = mapping.IRODSPath
	}

	if len(mappings) == 0 {
		return fmt.Errorf("no virtual path mapping is given")
	}
	return nil
}
