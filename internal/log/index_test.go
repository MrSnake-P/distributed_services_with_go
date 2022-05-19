package log

import (
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile("./", "index_test")
	//f, err := os.OpenFile("index_test", os.O_CREATE|os.O_RDWR, 0644)
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)
	t.Log(idx.size)

	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, w := range entries {
		err = idx.Write(w.Off, w.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(w.Off))
		require.NoError(t, err)
		require.Equal(t, w.Pos, pos)
	}

	// index and scanner should error when reading past existing entries
	// 超过现有索引数肯定报错
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	err = idx.Close()

	// index should build its state from the existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)

	idx.Close()
}
