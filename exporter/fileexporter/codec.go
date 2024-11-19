// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package fileexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter"

import (
	"bytes"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// compressFunc defines how to compress encoded telemetry data.
type compressFunc func(src []byte) []byte

var (
	encoder, _ = zstd.NewWriter(nil)

	lz4Encoder = lz4.NewWriter(nil)
)

var encoders = map[string]compressFunc{
	compressionZSTD:   zstdCompress,
	compressionSnappy: snappyCompress,
	compressionLZ4:    lz4Compress,
}

func buildCompressor(compression string) compressFunc {
	if compression == "" {
		return noneCompress
	}
	return encoders[compression]
}

// zstdCompress compress a buffer with zstd
func zstdCompress(src []byte) []byte {
	return encoder.EncodeAll(src, make([]byte, 0, len(src)))
}

func snappyCompress(src []byte) []byte {
	return snappy.Encode(src)
}

func lz4Compress(src []byte) []byte {
	var buf bytes.Buffer
	lz4Encoder.Reset(&buf)
	if _, err := lz4Encoder.Write(src); err != nil {
		return nil
	}
	return buf.Bytes()
}

// noneCompress return src
func noneCompress(src []byte) []byte {
	return src
}
