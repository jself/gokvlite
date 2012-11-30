package gokvlite

import (
	"container/list"
	"encoding/binary"
	"errors"
	"io"
	"os"
)

const freeBlockSize = 1024

type fileHeaderData struct {
	Freeblock_start int64
	Data_start      int64
}

type blockListHeaderData struct {
	Next int64
	Size int64
}

type blockListArrayEntryData struct {
	//This is a single array entry
	Free  uint8
	Start int64
	Size  int64
}

type blockListInfo struct {
	//This is a wrapper for a single array entry
	Entry    *blockListArrayEntryData
	Location int64
}

func (info *blockListInfo) writeInfo(writer io.WriterAt) error {
	return writeTo(writer, info.Location, info.Entry)
}

func (info *blockListInfo) ReadData(reader io.ReaderAt) (*[]byte, error) {
	//Reads the data from the location specific in the info/entry
	if info.Entry.Free > 0 {
		return nil, errors.New("filemanager: ReadData: Info is free, unable to read")
	}
	data := make([]byte, info.Entry.Size)
	_, err := reader.ReadAt(data, info.Entry.Start)
	return &data, err
}

func (info *blockListInfo) WriteData(writer io.WriterAt, data []byte) error {
	//Writes the data to the location specified by the info/entry
	if info.Entry.Free == 0 {
		return errors.New("Info is free, unable to write")
	}
	if int64(binary.Size(data)) != info.Entry.Size {
		return errors.New("Size of data is incorrect for size of info.")
	}
	_, err := writer.WriteAt(data, info.Entry.Start)
	return err
}

type blockListManager struct {
	headerStart int64
	header      *blockListHeaderData
}

type blockListInterface struct {
	/* This handles multiple block lists, gets a free block, etc
	 */

	fileheader     *fileHeaderData
	file           *os.File
	Blocklists     list.List
	Freeblocks     list.List
	Freeentries    list.List
	BlockListInfos map[int64]*blockListInfo
}

func (bli *blockListInterface) getFileEnd() (int64, error) {
	err := bli.file.Sync()
	if err != nil {
		return 0, err
	}

	fi, err := bli.file.Stat()
	if err != nil {
		return 0, err
	}

	end := fi.Size()
	return end, err
}

func (bli *blockListInterface) makeNewBlockList() error {
	end, err := bli.getFileEnd()
	if err != nil {
		return err
	}
	manager, _, err := bli.newBlockList(bli.file, end, freeBlockSize)
	if err != nil {
		return err
	}

	el := bli.Blocklists.Back()
	if el != nil {
		bl, ok := el.Value.(*blockListManager)
		if !ok {
			return errors.New("Invalid type for blocklistmanager in makeNewBlockList")
		}

		bl.header.Next = manager.headerStart
		err = writeTo(bli.file, bl.headerStart, bl.header)
		if err != nil {
			return err
		}
	}

	bli.Blocklists.PushBack(manager)
	return nil
}

func (bli *blockListInterface) getFreeEntry() (*blockListInfo, error) {
	//gets a free entry and removes it from the free entry list
	el := bli.Freeentries.Front()
	if el == nil {
		err := bli.makeNewBlockList()
		if err != nil {
			return nil, err
		}

		el = bli.Freeentries.Front()
		if el == nil {
			return nil, errors.New("Unable to get new element after making new blocklist")
		}
	}

	info, ok := el.Value.(*blockListInfo)
	if !ok {
		return info, errors.New("Invalid type in Freeentries list")
	}
	bli.Freeentries.Remove(el)

	return info, nil
}

func (bli *blockListInterface) Resize(info *blockListInfo, size int64) (*blockListInfo, *blockListInfo, error) {
	//resizes the info block, makes a new block, returns the old resized one and the new one
	startingsize := info.Entry.Size
	if startingsize == size {
		return nil, info, nil
	}

	if size > startingsize {
		//Just get a new entry and disregard the existing one
		newinfo, err := bli.GetFree(size)
		if err != nil {
			return nil, nil, err
		}
		err = bli.SetFree(info)
		if err != nil {
			return nil, nil, err
		}
		return nil, newinfo, nil
	}

	info.Entry.Size = size

	err := info.writeInfo(bli.file)
	if err != nil {
		return nil, nil, err
	}

	newinfo, err := bli.getFreeEntry()
	if err != nil {
		return nil, nil, err
	}

	newinfo.Entry.Size = startingsize - size
	newinfo.Entry.Free = 1
	newinfo.Entry.Start = info.Entry.Start + size
	err = newinfo.writeInfo(bli.file)
	if err != nil {
		return nil, nil, err
	}
	bli.Freeblocks.PushBack(newinfo)

	return info, newinfo, nil
}

func (bli *blockListInterface) SetFree(info *blockListInfo) error {
	//Sets a blocklist as free
	info.Entry.Free = 1
	err := info.writeInfo(bli.file)
	if err != nil {
		return err
	}
	bli.Freeblocks.PushBack(info)
	return nil
}

func (bli *blockListInterface) GetFree(size int64) (*blockListInfo, error) {
	//find a block large enough and resize it if it exists. Returns it marked as not free
	for e := bli.Freeblocks.Front(); e != nil; e = e.Next() {
		info, ok := e.Value.(*blockListInfo)
		if !ok {
			return nil, errors.New("Incorrect type in Freeblocks")
		}
		switch {
		case info.Entry.Size == size:
			info.Entry.Free = 0
			bli.Freeblocks.Remove(e)
			return info, nil

		case info.Entry.Size > size:
			_, _, err := bli.Resize(info, size)
			if err != nil {
				return nil, err
			}
			info.Entry.Free = 0
			bli.Freeblocks.Remove(e)
			return info, nil
		}
	}

	//append to end of file
	info, err := bli.getFreeEntry()
	if err != nil {
		return nil, err
	}

	end, err := bli.getFileEnd()
	if err != nil {
		return nil, err
	}

	info.Entry.Start = end
	info.Entry.Size = size
	info.Entry.Free = 0

	//Allocate the data storage
	data := make([]byte, size)
	bli.file.WriteAt(data, end)

	err = info.writeInfo(bli.file)
	return info, err
}

func (bli *blockListInterface) newBlockList(w io.WriterAt, start int64, size int64) (*blockListManager, int64, error) {
	//This creates a single unlinked blocklist
	//Size is in number of items, not bytes
	var written int64

	manager := new(blockListManager)

	header := new(blockListHeaderData)
	header.Size = size
	header.Next = 0
	err := writeTo(w, start, header)

	if err != nil {
		return manager, 0, err
	}

	written += int64(binary.Size(header))

	manager.header = header
	manager.headerStart = start

	for i := int64(0); i < size; i++ {
		entry := new(blockListArrayEntryData)
		entry.Free = 1
		info := blockListInfo{entry, written + start}
		err = info.writeInfo(w)
		if err != nil {
			return manager, written, err
		}
		bli.Freeentries.PushBack(&info)
		bli.BlockListInfos[info.Location] = &info
		written += int64(binary.Size(entry))

	}
	return manager, written, err
}

func (bli *blockListInterface) readBlockList(reader io.ReaderAt, start int64) (*blockListManager, error) {
	var read int64
	header := new(blockListHeaderData)
	err := readFrom(reader, start, header)
	if err != nil {
		return nil, err
	}

	blm := new(blockListManager)
	blm.header = header
	blm.headerStart = start

	read += int64(binary.Size(header))

	for i := int64(0); i < blm.header.Size; i++ {
		data := new(blockListArrayEntryData)
		err := readFrom(reader, start+read, data)
		if err != nil {
			return blm, err
		}

		info := blockListInfo{data, start + read}
		if data.Size == 0 {
			bli.Freeentries.PushBack(&info)
		} else if data.Free > 0 {
			bli.Freeentries.PushBack(&info)
		}
		bli.BlockListInfos[info.Location] = &info

		read += int64(binary.Size(data))
	}
	return blm, nil
}

func readFrom(r io.ReaderAt, start int64, data interface{}) error {
	//This function reads any of the structs above and returns it
	sr := io.NewSectionReader(r, start, int64(binary.Size(data)))
	return binary.Read(sr, binary.LittleEndian, data)
}

func writeTo(w io.WriterAt, start int64, data interface{}) error {
	//This function writes any of the structs above and returns it
	sw := newSectionWriter(w, start)
	return binary.Write(sw, binary.LittleEndian, data)
}

func newFile(path string) (*os.File, *blockListInterface, error) {
	//This function creates a new file and writes out the header and initial block list
	bli := new(blockListInterface)
	bli.BlockListInfos = make(map[int64]*blockListInfo)
	file, err := os.Create(path)
	if err != nil {
		return file, nil, err
	}

	bli.file = file
	header := fileHeaderData{0, 0}
	bli.fileheader = &header
	manager, written, err := bli.newBlockList(file, int64(binary.Size(header)), freeBlockSize)

	if err != nil {
		return file, bli, err
	}

	bli.Blocklists.PushBack(manager)

	header.Freeblock_start = int64(binary.Size(header))
	header.Data_start = header.Freeblock_start + written
	err = writeTo(file, 0, header)
	return file, bli, err
}

func readFile(path string) (*os.File, *blockListInterface, error) {
	bli := new(blockListInterface)
	bli.BlockListInfos = make(map[int64]*blockListInfo)
	file, err := os.Open(path)
	if err != nil {
		return file, nil, err
	}

	bli.file = file
	header := new(fileHeaderData)
	bli.fileheader = header
	if err = readFrom(file, 0, header); err != nil {
		return file, nil, err
	}
	blm, err := bli.readBlockList(file, header.Freeblock_start)
	bli.Blocklists.PushBack(blm)
	for blm.header.Next != 0 {
		blm, err = bli.readBlockList(file, blm.header.Next)
		if err != nil {
			return file, nil, err
		}
		bli.Blocklists.PushBack(blm)
	}

	return file, bli, err
}
