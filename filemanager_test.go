package gokvlite

import (
	"container/list"
	"encoding/binary"
	"io/ioutil"
	"testing"
)

func TestWriteRead(t *testing.T) {
	f, err := ioutil.TempFile("/tmp", "gotest")
	if err != nil {
		t.Fatalf("Unable to create temp file")
	}
	defer f.Close()
	var fhd fileHeaderData
	fhd.Freeblock_start = 10
	fhd.Data_start = 1000

	writeTo(f, 0, &fhd)

	var fhd2 fileHeaderData
	readFrom(f, 0, &fhd2)
	if fhd.Data_start != 1000 || fhd.Freeblock_start != 10 {
		t.Fatalf("Incorrect data on freeblock")
	}

	//Tests that we can write beyond the end of the file

	err = writeTo(f, 500, &fhd)
	if err != nil {
		t.Fatalf("Unable to write file header data", err)
	}
	err = readFrom(f, 500, &fhd2)
	if err != nil {
		t.Fatalf("Unable to read file header data: ", err)
	}

	if fhd.Data_start != 1000 {
		t.Fatalf("Incorrect data start")
	}

	//Tests writing an array data
	data := blockListArrayEntryData{0, 1, 20}
	err = writeTo(f, 0, &data)
	if err != nil {
		t.Fatalf("Error writing data: ", err)
	}
	var data2 blockListArrayEntryData
	if err = readFrom(f, 0, &data2); err != nil {
		t.Fatalf("Error read data")
	}
	if data2.Start != 1 || data2.Size != 20 || data2.Free > 0 {
		t.Fatalf("Incorrect data read back from list entry data: ", data2)
	}

}

func TestFile(t *testing.T) {
	// Tests that writing a new file works
	filename := "/tmp/gotest"
	file, bli, err := newFile(filename)
	if err != nil {
		t.Fatalf("Received error in new function:", err)

	}
	defer file.Close()

	el := bli.Blocklists.Front()
	if el == nil {
		t.Fatalf("El is nil")
	}
	manager, ok := el.Value.(*blockListManager)
	if !ok {
		t.Fatalf("Incorrect type received from Blocklists")
	}

	var entry *blockListInfo
	for _, entry = range bli.BlockListInfos {
		break
	}
	entry.Entry.Free = 0
	entry.Entry.Size = 600
	location := entry.Location
	entry.writeInfo(file)

	file.Close()
	file, bli2, err := readFile(filename)
	manager2 := bli2.Blocklists.Front().Value.(*blockListManager)
	if manager.headerStart != manager2.headerStart || manager.header.Size != manager2.header.Size {
		t.Fatalf("Invalid headers")
	}
	entry2 := bli2.BlockListInfos[location]
	if entry2.Entry.Free != 0 || entry2.Entry.Size != 600 {
		t.Fatalf("Entry was read incorrectly")
	}
}

func TestGetFree(t *testing.T) {
	filename := "/tmp/gotest"
	file, bli, err := newFile(filename)
	if err != nil {
		t.Fatalf("Received error in new function:", err)

	}
	defer file.Close()

	info, err := bli.GetFree(1024)
	if err != nil {
		t.Fatalf("Received error getting free element:", err)
	}

	if info.Entry.Size != 1024 {
		t.Fatalf("Incorrect Size for Entry")
	}

	err = bli.SetFree(info)
	if err != nil {
		t.Fatalf("Error in SetFree:", err)
	}

	info2, err := bli.GetFree(1000)
	if err != nil {
		t.Fatalf("Received error getting free element:", err)
	}

	if info2.Location != info.Location {
		t.Fatalf("Location doesn't match freed info")
	}
	if info2.Entry.Size != 1000 {
		t.Fatalf("Incorrect Size for Entry 2: ", info2.Entry.Size)
	}

	info3, err := bli.GetFree(1200)

	if info3.Entry.Start == info.Entry.Start {
		t.Fatalf("Starts match when they should be different")
	}
}

func TestMakeNewBlockList(t *testing.T) {
	filename := "/tmp/gotest"
	file, bli, err := newFile(filename)
	if err != nil {
		t.Fatalf("Received error in new function:", err)

	}
	defer file.Close()

	l := list.List{}
	bli.Freeentries = l

	info, err := bli.GetFree(1024)
	if err != nil {
		t.Fatalf("Received error from GetFree: ", err)
	}
	if info.Entry.Size != 1024 {
		t.Fatalf("Incorrect size for Entry")
	}

	if bli.Blocklists.Len() != 2 {
		t.Fatalf("New blocklist was not created. Size: ", bli.Blocklists.Len())
	}

	file.Close()
	file, bli, err = readFile(filename)
	defer file.Close()

	if bli.Blocklists.Len() != 2 {
		t.Fatalf("Incorrect number of blocklists: ", bli.Blocklists.Len())
	}

}

//These are sanity tests to make sure that Go works like I think it does
func TestPointerSize(t *testing.T) {
	//Tests that a binary.Size of a pointer works like a non-pointer
	var e blockListArrayEntryData
	entry := new(blockListArrayEntryData)
	if binary.Size(e) != binary.Size(entry) {
		t.Fatalf("binary.Size of a pointer isn't the same as the struct size")
	}
}
