package appinstance

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/google/uuid"
)

type appResult struct {
	output []byte
	err    error
}

type AppRequest struct {
	Id       uuid.UUID
	FuncName string
	Args     [][]byte

	outputBuf bytes.Buffer

	ctx      context.Context
	instance *AppInstance
	done     chan appResult
}

type appReqInput struct {
	Id                uuid.UUID
	WorldRank         uint32
	WorldSize         uint32
	InputFilesOffset  uint32
	InputFilesCount   uint32
	OutputFilesOffset uint32
	OutputFilesCount  uint32
	FuncNameOffset    uint32
	ArgsOffset        uint32
	ArgsCount         uint32
	_                 [76]byte
}

type appEnvIovec struct {
	Offset uint32
	Length uint32
}

func init() {
	if binary.Size(&appReqInput{}) != 128 {
		panic("App request input must be 128 bytes long")
	}
}

func (request *AppRequest) Wait() ([]byte, error) {
	result := <-request.done
	if result.err != nil {
		return nil, result.err
	}
	return result.output, nil
}
