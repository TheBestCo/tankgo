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
		v *uint64
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
			err := rb.ReadUint64(tt.args.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadBuffer.ReadUint64() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
