package gokvlite

import (
	"os"
)

//Opens a file to be used as a database. If the file doesn't exist,
//it'll create it and initialize it.
func Open(filename string) (*KeyHandler, error) {
	_, bli, err := readFile(filename)
	if err != nil {
		if e, ok := err.(*os.PathError); ok && (os.IsNotExist(e)) {
			//file doesn't exist, create
			_, bli, err = newFile(filename)
			if err != nil {
				return nil, err
			}

			var kh KeyHandler
			kh.bli = bli
			kh.datalocs = make(map[string]*keyInfo)

			err = kh.makeNewList()
			return &kh, err
		} else {
			return nil, err
		}
	}
	var kh KeyHandler
	kh.bli = bli
	kh.datalocs = make(map[string]*keyInfo)
	err = kh.readFile(bli.fileheader.Data_start)
	return &kh, err
}
