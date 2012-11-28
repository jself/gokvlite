package main

import (
    "os"
	"testing"
	"fmt"
)


func TestOpen(t *testing.T) {
	tempfile := "/tmp/gotest"
    os.Remove(tempfile)
	kh, err := Open(tempfile)

	if err != nil {
		t.Fatalf("Error: ", err)
	}

	err = kh.Set("Testing", []byte("blah"))
	if err != nil {
		t.Fatalf("Error: ", err)
	}

	err, data := kh.Get("Testing")
	if err != nil {
		t.Fatalf("Error: ", err)
	}
	if string(*data) != "blah" {
		t.Fatalf("Didn't receive the same thing we set")
	}
	if err = kh.Close(); err != nil {
		t.Fatal("Error: ", err)
	}

	kh, err = Open(tempfile)
	if err != nil {
		t.Fatalf("Error: ", err)
	}
	defer kh.Close()


	err, data = kh.Get("Testing")
	if err != nil { t.Fatalf("Error: ", err)}
	if string(*data) != "blah" {
		t.Fatalf("Invalid data")
	}
}

func makeUuid() string {
	f, _ := os.Open("/dev/urandom")
	defer f.Close()
	b := make([]byte, 16)
	f.Read(b)
	uuid := fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
	return uuid
}

func TestMultiple(t *testing.T) {
	tempfile := "/tmp/gotest"
    os.Remove(tempfile)
	kh, err := Open(tempfile)
	if err != nil {
		t.Fatalf("Error: ", err)
	}
	for i := 0; i <= keyblocksize+20; i++ {
		key := makeUuid()
		var data *[]byte
		err := kh.Set(key, []byte(key))
		if err != nil { t.Fatalf("Error in setting key", err)}
		if err, data = kh.Get(key); err != nil { t.Fatalf("Error: ", err)}
		if string(*data) != key { t.Fatalf("Data not equal to set value")}
	}
}
