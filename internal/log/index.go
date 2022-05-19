package log

import (
	"github.com/edsrzf/mmap-go"
	"io"
	"os"
)

const (
	// 索引号记录位置
	offWidth uint64 = 4
	// 索引记录的值
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap mmap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	// 首先将文件截断至最大索引处
	idx.size = uint64(fi.Size())
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	// 文件的内存储存映射
	if idx.mmap, err = mmap.Map(
		f,
		mmap.RDWR,
		0,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

func (i *index) Close() error {
	// make sure that has persisted file
	// win10 error: FlushFileBuffers:handler is invalid
	//if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
	//	return err
	//}
	if err := i.mmap.Unmap(); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	// 将持久化文件截断为实际包含的数据量
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		// 返回最后一个索引
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	// relative offsets
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	//  validate that we have space to write the entry
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += entWidth
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
