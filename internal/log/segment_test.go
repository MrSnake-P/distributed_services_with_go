package log

import (
	api "distributed_services_with_go/github.com/MrSnake-P/api/log_v1"
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("testdata/", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.baseOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		record, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, record.Value)
	}

	// max index
	_, err = s.Append(want)
	t.Log(err)
	require.Equal(t, io.EOF, err)
	require.True(t, s.IsMaxed())
	// 需要关闭对文件的占用否则之后的测试将会不通过
	s.Close()

	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	// max store
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)

	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
