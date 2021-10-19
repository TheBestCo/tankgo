package binary

import (
	"bufio"
	"testing"
)

func TestReadBuffer_ReadUint64(t *testing.T) {
	type fields struct {
		reader bufio.Reader
		Parser Parser
	}
	type args struct {
		limit int
		v     *uint64
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := &ReadBuffer{
				reader: tt.fields.reader,
				Parser: tt.fields.Parser,
			}
			got, err := rb.ReadUint64(tt.args.limit, tt.args.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadBuffer.ReadUint64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ReadBuffer.ReadUint64() = %v, want %v", got, tt.want)
			}
		})
	}
}
