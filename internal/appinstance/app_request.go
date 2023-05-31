// Copyright 2023 The Dots Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	InputFilesOffset  uint32
	InputFilesCount   uint32
	OutputFilesOffset uint32
	OutputFilesCount  uint32
	FuncNameOffset    uint32
	ArgsOffset        uint32
	ArgsCount         uint32
	_                 [84]byte
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
