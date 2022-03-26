package report

import (
	"fmt"
	"sync"
	"time"

	"github.com/cyverse/irodsfs-common/irods"
	"github.com/cyverse/irodsfs-common/util"
	monitor_client "github.com/cyverse/irodsfs-monitor/client"
	monitor_types "github.com/cyverse/irodsfs-monitor/types"
	log "github.com/sirupsen/logrus"
)

// IRODSFSRestReporter reports metrics to REST monitoring service
type IRODSFSRestReporter struct {
	monitorURL          string
	client              *monitor_client.APIClient
	failed              bool
	ignoreError         bool
	maxTransferBlockLen int
}

type IRODSFSInstanceRestReporter struct {
	reporter          *IRODSFSRestReporter
	instanceID        string
	fileTransferMap   map[string]*monitor_types.ReportFileTransfer
	nextFileOffsetMap map[string]int64
	mutex             sync.Mutex // lock for FileTransferMap and NextFileOffsetMap
}

// NewIRODSFSRestReporter creates a new REST monitoring reporter
func NewIRODSFSRestReporter(monitorURL string, ignoreError bool, maxTransferBlockLen int, requestTimeoutSec int) IRODSFSReportClient {
	monitoringClient := monitor_client.NewAPIClient(monitorURL, time.Second*time.Duration(requestTimeoutSec))

	return &IRODSFSRestReporter{
		monitorURL:          monitorURL,
		failed:              false,
		client:              monitoringClient,
		ignoreError:         ignoreError,
		maxTransferBlockLen: maxTransferBlockLen,
	}
}

// Release releases resources used
func (reporter *IRODSFSRestReporter) Release() {
}

// StartInstance reports start of a new iRODS FS Client instance
func (reporter *IRODSFSRestReporter) StartInstance(instance *monitor_types.ReportInstance) (IRODSFSInstanceReportClient, error) {
	logger := log.WithFields(log.Fields{
		"package":  "report",
		"struct":   "IRODSFSRestReporter",
		"function": "StartInstance",
	})

	defer util.StackTraceFromPanic(logger)

	if reporter.failed {
		if reporter.ignoreError {
			// return dummy
			return &IRODSFSInstanceRestReporter{
				reporter:          reporter,
				instanceID:        "", // dummy
				fileTransferMap:   map[string]*monitor_types.ReportFileTransfer{},
				nextFileOffsetMap: map[string]int64{},
				mutex:             sync.Mutex{},
			}, nil
		}

		errorMessage := "failed to report the instance to monitoring service"
		logger.Error(errorMessage)
		return nil, fmt.Errorf(errorMessage)
	}

	instanceID, err := reporter.client.AddInstance(instance)
	if err != nil {
		errorMessage := "failed to report the instance to monitoring service"
		logger.WithError(err).Error(errorMessage)
		reporter.failed = true

		if reporter.ignoreError {
			// return dummy
			return &IRODSFSInstanceRestReporter{
				reporter:          reporter,
				instanceID:        "", // dummy
				fileTransferMap:   map[string]*monitor_types.ReportFileTransfer{},
				nextFileOffsetMap: map[string]int64{},
				mutex:             sync.Mutex{},
			}, nil
		} else {
			return nil, fmt.Errorf(errorMessage)
		}
	}

	return &IRODSFSInstanceRestReporter{
		reporter:          reporter,
		instanceID:        instanceID,
		fileTransferMap:   map[string]*monitor_types.ReportFileTransfer{},
		nextFileOffsetMap: map[string]int64{},
		mutex:             sync.Mutex{},
	}, nil
}

// Terminate reports termination of the iRODS FS instance
func (reporter *IRODSFSInstanceRestReporter) Terminate() error {
	logger := log.WithFields(log.Fields{
		"package":  "report",
		"struct":   "IRODSFSInstanceRestReporter",
		"function": "Terminate",
	})

	defer util.StackTraceFromPanic(logger)

	if reporter.reporter.failed {
		if reporter.reporter.ignoreError {
			return nil
		}

		errorMessage := fmt.Sprintf("failed to report termination of the instance %s to monitoring service", reporter.instanceID)
		logger.Error(errorMessage)
		return fmt.Errorf(errorMessage)
	}

	if len(reporter.instanceID) == 0 {
		if reporter.reporter.ignoreError {
			return nil
		} else {
			errorMessage := "instance is not initialized"
			logger.Error(errorMessage)
			return fmt.Errorf(errorMessage)
		}
	}

	err := reporter.reporter.client.TerminateInstance(reporter.instanceID)
	if err != nil {
		if reporter.reporter.ignoreError {
			return nil
		}

		errorMessage := "failed to report termination of the instance to monitoring service"
		logger.WithError(err).Error(errorMessage)
		reporter.reporter.failed = true
		return fmt.Errorf(errorMessage)
	}

	return nil
}

func (reporter *IRODSFSInstanceRestReporter) makeFileTransferKey(fileHandle irods.IRODSFSFileHandle) string {
	return fmt.Sprintf("%s:%s", fileHandle.GetEntry().Path, fileHandle.GetID())
}

// StartFileAccess reports a start of file access
func (reporter *IRODSFSInstanceRestReporter) StartFileAccess(fileHandle irods.IRODSFSFileHandle) error {
	logger := log.WithFields(log.Fields{
		"package":  "report",
		"struct":   "IRODSFSInstanceRestReporter",
		"function": "StartFileAccess",
	})

	fileEntry := fileHandle.GetEntry()

	if reporter.reporter.failed {
		if reporter.reporter.ignoreError {
			return nil
		}

		errorMessage := fmt.Sprintf("failed to report start of access to file %s for instance %s to monitoring service", fileEntry.Path, reporter.instanceID)
		logger.Error(errorMessage)
		return fmt.Errorf(errorMessage)
	}

	if len(reporter.instanceID) == 0 {
		if reporter.reporter.ignoreError {
			return nil
		} else {
			errorMessage := "instance is not initialized"
			logger.Error(errorMessage)
			return fmt.Errorf(errorMessage)
		}
	}

	transferReport := &monitor_types.ReportFileTransfer{
		InstanceID: reporter.instanceID,

		FilePath:     fileEntry.Path,
		FileSize:     fileEntry.Size,
		FileOpenMode: string(fileHandle.GetOpenMode()),

		TransferBlocks:     make([]monitor_types.FileBlock, 0, reporter.reporter.maxTransferBlockLen),
		TransferSize:       0,
		LargestBlockSize:   0,
		SmallestBlockSize:  0,
		TransferBlockCount: 0,
		SequentialAccess:   true,

		FileOpenTime: time.Now().UTC(),
	}

	key := reporter.makeFileTransferKey(fileHandle)

	reporter.mutex.Lock()
	defer reporter.mutex.Unlock()

	reporter.fileTransferMap[key] = transferReport
	reporter.nextFileOffsetMap[key] = 0

	return nil
}

// FileAccess reports a file access
func (reporter *IRODSFSInstanceRestReporter) FileAccess(fileHandle irods.IRODSFSFileHandle, offset int64, length int64) error {
	logger := log.WithFields(log.Fields{
		"package":  "report",
		"struct":   "IRODSFSInstanceRestReporter",
		"function": "StartFileAccess",
	})

	fileEntry := fileHandle.GetEntry()

	if reporter.reporter.failed {
		if reporter.reporter.ignoreError {
			return nil
		}

		errorMessage := fmt.Sprintf("failed to report access to file %s for instance %s to monitoring service", fileEntry.Path, reporter.instanceID)
		logger.Error(errorMessage)
		return fmt.Errorf(errorMessage)
	}

	if len(reporter.instanceID) == 0 {
		if reporter.reporter.ignoreError {
			return nil
		}

		errorMessage := "instance is not initialized"
		logger.Error(errorMessage)
		return fmt.Errorf(errorMessage)
	}

	key := reporter.makeFileTransferKey(fileHandle)

	reporter.mutex.Lock()
	defer reporter.mutex.Unlock()

	if transfer, ok := reporter.fileTransferMap[key]; ok {
		block := monitor_types.FileBlock{
			Offset:     offset,
			Length:     length,
			AccessTime: time.Now().UTC(),
		}

		transfer.TransferSize += int64(length)
		transfer.TransferBlockCount++
		if transfer.LargestBlockSize < length {
			transfer.LargestBlockSize = length
		}

		if transfer.SmallestBlockSize == 0 {
			transfer.SmallestBlockSize = length
		} else if transfer.SmallestBlockSize > length {
			transfer.SmallestBlockSize = length
		}

		strictSequential := false
		mostlySequential := false
		if nextOffset, ok2 := reporter.nextFileOffsetMap[key]; ok2 {
			strictSequential, mostlySequential = reporter.checkSequentialTransfer(nextOffset, offset, length)
		}

		reporter.nextFileOffsetMap[key] = offset + length

		if !mostlySequential {
			transfer.SequentialAccess = false
		}

		if len(transfer.TransferBlocks) < reporter.reporter.maxTransferBlockLen {
			if len(transfer.TransferBlocks) == 0 {
				transfer.TransferBlocks = append(transfer.TransferBlocks, block)
			} else {
				if strictSequential {
					// merge to last
					lastBlock := transfer.TransferBlocks[len(transfer.TransferBlocks)-1]
					lastBlock.Length += block.Length

					transfer.TransferBlocks[len(transfer.TransferBlocks)-1] = lastBlock
				} else {
					transfer.TransferBlocks = append(transfer.TransferBlocks, block)
				}
			}
		}
	} else {
		if reporter.reporter.ignoreError {
			return nil
		}

		errorMessage := fmt.Sprintf("failed to find file transfer info for path %s, handle id %s", fileEntry.Path, fileHandle.GetID())
		logger.Error(errorMessage)
		return fmt.Errorf(errorMessage)
	}

	return nil
}

// DoneFileAccess reports that the file transfer is done
func (reporter *IRODSFSInstanceRestReporter) DoneFileAccess(fileHandle irods.IRODSFSFileHandle) error {
	logger := log.WithFields(log.Fields{
		"package":  "report",
		"struct":   "IRODSFSInstanceRestReporter",
		"function": "DoneFileAccess",
	})

	defer util.StackTraceFromPanic(logger)

	fileEntry := fileHandle.GetEntry()

	if reporter.reporter.failed {
		if reporter.reporter.ignoreError {
			return nil
		}

		errorMessage := fmt.Sprintf("failed to report access to file %s for instance %s to monitoring service", fileEntry.Path, reporter.instanceID)
		logger.Error(errorMessage)
		return fmt.Errorf(errorMessage)
	}

	if len(reporter.instanceID) == 0 {
		if reporter.reporter.ignoreError {
			return nil
		}

		errorMessage := "instance is not initialized"
		logger.Error(errorMessage)
		return fmt.Errorf(errorMessage)
	}

	key := reporter.makeFileTransferKey(fileHandle)

	reporter.mutex.Lock()
	defer reporter.mutex.Unlock()

	if transfer, ok := reporter.fileTransferMap[key]; ok {
		transfer.FileCloseTime = time.Now().UTC()

		err := reporter.reporter.client.AddFileTransfer(transfer)
		if err != nil {
			if !reporter.reporter.ignoreError {
				return err
			}

			errorMessage := fmt.Sprintf("failed to report file transfer for path %s to monitoring service", fileEntry.Path)
			logger.WithError(err).Error(errorMessage)
			reporter.reporter.failed = true
			return fmt.Errorf(errorMessage)
		}

		delete(reporter.fileTransferMap, key)
		delete(reporter.nextFileOffsetMap, key)
	} else {
		if reporter.reporter.ignoreError {
			return nil
		}

		errorMessage := fmt.Sprintf("failed to find file transfer info for path %s, handle id %s", fileEntry.Path, fileHandle.GetID())
		logger.Error(errorMessage)
		return fmt.Errorf(errorMessage)
	}

	return nil
}

// checkSequentialTransfer determines if the given file transfer is sequential transfer or not
// first return val: true if this is strictly sequential
// second return val: true if this is generally sequential
func (reporter *IRODSFSInstanceRestReporter) checkSequentialTransfer(expectedOffset int64, transferOffset int64, transferLength int64) (bool, bool) {
	// 1 => 2 => 3 block order
	if expectedOffset == transferOffset {
		return true, true
	}

	// concurrent file transfer may make serial file access slightly unordered.
	// allow 3 => 1 => 2 block transfer order
	offsetDelta := expectedOffset - transferOffset
	if offsetDelta < 0 {
		offsetDelta *= -1
	}

	if offsetDelta <= transferLength*2 {
		return false, true
	}

	return false, false
}
