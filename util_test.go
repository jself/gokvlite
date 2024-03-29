package gokvlite

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestSectionWriter(t *testing.T) {
	f, err := ioutil.TempFile("/tmp", "gotest")
	if err != nil {
		t.Fatalf("Error in TestSectionWriter:", err)
	}
	defer f.Close()

	f.WriteString("Testing!")

	var buf bytes.Buffer
	buf.WriteString("Testing!")
	sectionWriter := newSectionWriter(f, 2)
	sectionWriter.Write(buf.Bytes())

	f.Seek(0, 0)
	stat, err := f.Stat()
	if err != nil {
		t.Fatalf("Unable to stat tmp file")
	}

	b := make([]byte, stat.Size())
	f.Read(b)
	if string(b) != "TeTesting!" {
		t.Errorf("Incorrect output for written text: ", string(b))
	}
}
