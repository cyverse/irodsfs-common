package report

import (
	"github.com/cyverse/irodsfs-common/irods"
	monitor_types "github.com/cyverse/irodsfs-monitor/types"
)

// IRODSFSReportClient is a client interface to report I/O stats
type IRODSFSReportClient interface {
	Release()

	StartInstance(instance *monitor_types.ReportInstance) (IRODSFSInstanceReportClient, error)
}

// IRODSFSInstanceReportClient is a client interface to report I/O stats of an instance
type IRODSFSInstanceReportClient interface {
	Terminate() error

	StartFileAccess(fileHandle irods.IRODSFSFileHandle) error
	FileAccess(fileHandle irods.IRODSFSFileHandle, offset int64, size int64) error
	DoneFileAccess(fileHandle irods.IRODSFSFileHandle) error
}
