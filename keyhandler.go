package gokvlite

import (
	"container/list"
	"encoding/binary"
	"errors"
)

const keyblocksize = 500

type keyArrayHeader struct {
	Next int64
	Size int64
}

type keyArrayHeaderInfo struct {
	Header   *keyArrayHeader
	Location int64
}

type keyInfo struct {
	//This is the in memory representation of the key/data
	Location int64
	Key      *blockListInfo
	Data     *blockListInfo
}

type keyEntry struct {
	//This represents the key/data in the array on disk for reading when building the index
	Free    uint8
	Keyloc  int64
	Dataloc int64
}

func (ki *keyInfo) Free(kh *KeyHandler) error {
	//Frees a key (delete)
	bli := kh.bli
	err := bli.SetFree(ki.Key)
	if err != nil {
		return err
	}
	err = bli.SetFree(ki.Data)
	if err != nil {
		return err
	}

	ke := keyEntry{1, 0, 0}
	err = writeTo(bli.file, ki.Location, ke)
	if err == nil {
		return err
	}
	ki.Key = nil
	ki.Data = nil
	kh.freeKeyInfos.PushBack(&ki)
	return nil
}

func (ki *keyInfo) Update(bli *blockListInterface, key string, data []byte) error {
	var err error
	save := false
	keysize := int64(binary.Size([]byte(key)))
	datasize := int64(binary.Size(data))
	if ki.Key == nil {
		save = true
		ki.Key, err = bli.GetFree(keysize)
		if err != nil {
			return err
		}
	} else if keysize != ki.Key.Entry.Size {
		//have to resize if the size of the string or data don't match the current size
		ki.Key, _, err = bli.Resize(ki.Key, keysize)
		if err != nil {
			return err
		}
		save = true
	}

	if ki.Data == nil {
		save = true
		ki.Data, err = bli.GetFree(datasize)
		if err != nil {
			return err
		}
	} else if datasize != ki.Data.Entry.Size {
		ki.Data, _, err = bli.Resize(ki.Data, datasize)
		if err != nil {
			return err
		}
		save = true
	}

	err = writeTo(bli.file, ki.Key.Entry.Start, []byte(key))
	if err != nil {
		return err
	}
	err = writeTo(bli.file, ki.Data.Entry.Start, data)
	if err != nil {
		return err
	}

	ke := keyEntry{0, ki.Key.Location, ki.Data.Location}
	if save {
		err = writeTo(bli.file, ki.Location, ke)
		return err
	}
	return nil
}

//This struct actually sets/gets/deletes a key from the database
type KeyHandler struct {
	datalocs     map[string]*keyInfo
	bli          *blockListInterface
	freeKeyInfos list.List
	keyHeaders   list.List
}

func (kh *KeyHandler) makeNewList() error {
	header := keyArrayHeader{0, keyblocksize}
	blankKeyEntry := keyEntry{1, 0, 0}
	size := int64(binary.Size(header)) + (int64(binary.Size(blankKeyEntry)) * keyblocksize)
	free, err := kh.bli.GetFree(size)
	if err != nil {
		return err
	}

	offset := free.Entry.Start
	//write the header
	err = writeTo(kh.bli.file, offset, header)
	if err != nil {
		return err
	}
	offset += int64(binary.Size(header))

	//write the entries and create the free infos to write out
	entrysize := int64(binary.Size(blankKeyEntry))
	for i := 0; i < keyblocksize; i++ {
		info := keyInfo{offset, nil, nil}
		err = writeTo(kh.bli.file, offset, blankKeyEntry)
		if err != nil {
			return err
		}
		offset += entrysize
		kh.freeKeyInfos.PushBack(&info)
	}

	headerinfo := keyArrayHeaderInfo{&header, free.Entry.Start}
	el := kh.keyHeaders.Back()
	if el == nil {
		//empty list
		kh.keyHeaders.PushBack(&headerinfo)
		//update the file header since this is the first list
		var fileheader fileHeaderData
		err := readFrom(kh.bli.file, 0, &fileheader)
		if err != nil {
			return err
		}
		fileheader.Data_start = headerinfo.Location
		err = writeTo(kh.bli.file, 0, &fileheader)
		return err
	}
	last, ok := el.Value.(*keyArrayHeaderInfo)
	if !ok {
		return errors.New("Invalid type for headerinfo in makenewlist:")
	}
	last.Header.Next = headerinfo.Location
	err = writeTo(kh.bli.file, last.Location, last.Header)
	if err != nil {
		return err
	}
	kh.keyHeaders.PushBack(&headerinfo)
	return nil
}

func (kh *KeyHandler) readFile(start int64) error {
	//Reads a file and builds out the keyHandler. Expects that bli exists on the KeyHandler.
	header := keyArrayHeader{0, keyblocksize}
	err := readFrom(kh.bli.file, start, &header)
	if err != nil {
		return err
	}

	headerInfo := keyArrayHeaderInfo{&header, start}
	kh.keyHeaders.PushBack(&headerInfo)
	offset := int64(binary.Size(header))

	entry := keyEntry{}
	for i := int64(0); i < header.Size; i++ {
		err := readFrom(kh.bli.file, start+offset, &entry)
		if err != nil {
			return err
		}

		if entry.Free > 0 {
			//entry is free, append to free infos
			info := keyInfo{start + offset, nil, nil}
			kh.freeKeyInfos.PushBack(&info)
		} else {
			//info has data, read it and set it in the key handler
			keybli, ok := kh.bli.BlockListInfos[entry.Keyloc]
			if !ok {
				return errors.New("keyhandler: readFile: Location not found for key")
			}
			databli, ok := kh.bli.BlockListInfos[entry.Dataloc]
			if !ok {
				return errors.New("keyhandler: readFile: Location not found for data")
			}

			data, err := keybli.ReadData(kh.bli.file)
			if err != nil {
				return err
			}
			key := string(*data)

			//don't need to read the data since it's read during Get()
			info := keyInfo{start + offset, keybli, databli}
			kh.datalocs[key] = &info
			offset += int64(binary.Size(entry))
		}
	}
	if header.Next > 0 {
		return kh.readFile(header.Next)
	}
	return nil
}

//Sets the key to data
func (kh *KeyHandler) Set(key string, data []byte) error {
	info, present := kh.datalocs[key]
	if present {
		return info.Update(kh.bli, key, data)
	}

	el := kh.freeKeyInfos.Front()

	if el == nil {
		err := kh.makeNewList()
		if err != nil {
			return err
		}
		el = kh.freeKeyInfos.Front()
		if el == nil {
			return errors.New("Unable to get a free key info after creating new")
		}

	}

	info, ok := el.Value.(*keyInfo)
	if !ok {
		return errors.New("Invalid type in freeKeyInfos list")
	}
	kh.freeKeyInfos.Remove(el)
	err := info.Update(kh.bli, key, data)
	if err != nil {
		return err
	}
	kh.datalocs[key] = info
	return nil
}

//Gets the data contained at string
func (kh *KeyHandler) Get(key string) (*[]byte, error) {
	info, ok := kh.datalocs[key]
	if !ok {
		return nil, errors.New("keyhandler: Get: Key does not exist")
	}

	bl := info.Data
	data := make([]byte, bl.Entry.Size)
	_, err := kh.bli.file.ReadAt(data, bl.Entry.Start)
	return &data, err
}

//Deletes the key if it exists. (returns if it doesn't)
func (kh *KeyHandler) Del(key string) error {
	info, ok := kh.datalocs[key]
	if !ok {
		return nil
	}

	delete(kh.datalocs, key)
	kh.freeKeyInfos.PushBack(info)
	return info.Free(kh)
}

//Closes the file returned by Open, can be deferred that way
func (kh *KeyHandler) Close() error {
	return kh.bli.file.Close()
}
