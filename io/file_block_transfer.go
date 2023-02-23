package io

import (
	"bytes"
	"io"
	"sync"
)

type FileBlockTransfer struct {
	blockID   int64
	buffer    *bytes.Buffer
	eof       bool // is eof?
	completed bool // is transfer completed?
	failed    bool // is transfer failed?
	mutex     sync.Mutex
	condition *sync.Cond
}

func NewFileBlockTransfer(blockID int64) *FileBlockTransfer {
	transfer := &FileBlockTransfer{
		blockID:   blockID,
		buffer:    &bytes.Buffer{},
		eof:       false,
		completed: false,
		failed:    false,
		mutex:     sync.Mutex{},
	}

	transfer.condition = sync.NewCond(&transfer.mutex)
	return transfer
}

func (transfer *FileBlockTransfer) GetBlockID() int64 {
	transfer.mutex.Lock()
	defer transfer.mutex.Unlock()

	return transfer.blockID
}

func (transfer *FileBlockTransfer) MarkFailed() {
	transfer.mutex.Lock()
	defer transfer.mutex.Unlock()

	if !transfer.completed {
		transfer.failed = true
	}

	transfer.condition.Broadcast()
}

func (transfer *FileBlockTransfer) MarkCompleted(eof bool) {
	transfer.mutex.Lock()
	defer transfer.mutex.Unlock()

	transfer.completed = true
	transfer.failed = false
	transfer.eof = eof
	transfer.condition.Broadcast()
}

func (transfer *FileBlockTransfer) IsFailed() bool {
	transfer.mutex.Lock()
	defer transfer.mutex.Unlock()

	return transfer.failed
}

func (transfer *FileBlockTransfer) IsCompleted() bool {
	transfer.mutex.Lock()
	defer transfer.mutex.Unlock()

	return transfer.completed
}

func (transfer *FileBlockTransfer) IsEOF() bool {
	transfer.mutex.Lock()
	defer transfer.mutex.Unlock()

	return transfer.eof
}

func (transfer *FileBlockTransfer) GetBuffer() *bytes.Buffer {
	transfer.mutex.Lock()
	defer transfer.mutex.Unlock()

	return transfer.buffer
}

func (transfer *FileBlockTransfer) GetBufferLen() int {
	transfer.mutex.Lock()
	defer transfer.mutex.Unlock()

	return transfer.buffer.Len()
}

func (transfer *FileBlockTransfer) CopyTo(buffer []byte, offset int) (int, error) {
	transfer.mutex.Lock()
	defer transfer.mutex.Unlock()

	bufferBytes := transfer.buffer.Bytes()
	copiedLen := copy(buffer, bufferBytes[offset:])

	if transfer.completed && transfer.eof {
		if offset+copiedLen >= transfer.buffer.Len() {
			return copiedLen, io.EOF
		}
	}

	return copiedLen, nil
}

func (transfer *FileBlockTransfer) Write(buffer []byte) {
	transfer.mutex.Lock()
	defer transfer.mutex.Unlock()

	transfer.buffer.Write(buffer)
}

func (transfer *FileBlockTransfer) WaitForData(size int) bool {
	transfer.mutex.Lock()
	defer transfer.mutex.Unlock()

	for transfer.buffer.Len() < size {
		if transfer.completed {
			return true
		}

		if transfer.failed {
			return false
		}

		transfer.condition.Wait()
	}

	return true
}

type FileBlockTransferMap struct {
	transfers map[int64]*FileBlockTransfer
	mutex     sync.Mutex
}

func NewFileBlockTransferMap() *FileBlockTransferMap {
	return &FileBlockTransferMap{
		transfers: map[int64]*FileBlockTransfer{},
		mutex:     sync.Mutex{},
	}
}

func (transferMap *FileBlockTransferMap) Put(transfer *FileBlockTransfer) {
	transferMap.mutex.Lock()
	defer transferMap.mutex.Unlock()

	transferMap.transfers[transfer.blockID] = transfer
}

func (transferMap *FileBlockTransferMap) Remove(blockID int64) {
	transferMap.mutex.Lock()
	defer transferMap.mutex.Unlock()

	delete(transferMap.transfers, blockID)
}

func (transferMap *FileBlockTransferMap) Clean() {
	transferMap.mutex.Lock()
	defer transferMap.mutex.Unlock()

	transferMap.transfers = map[int64]*FileBlockTransfer{}
}

func (transferMap *FileBlockTransferMap) Contains(blockID int64) bool {
	transferMap.mutex.Lock()
	defer transferMap.mutex.Unlock()

	_, ok := transferMap.transfers[blockID]
	return ok
}

func (transferMap *FileBlockTransferMap) Get(blockID int64) *FileBlockTransfer {
	transferMap.mutex.Lock()
	defer transferMap.mutex.Unlock()

	if transfer, ok := transferMap.transfers[blockID]; ok {
		return transfer
	}
	return nil
}

func (transferMap *FileBlockTransferMap) StopAllTransfers() {
	transferMap.mutex.Lock()
	defer transferMap.mutex.Unlock()

	for _, transfer := range transferMap.transfers {
		transfer.MarkFailed()
	}
}
