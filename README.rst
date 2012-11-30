=====
GoKVLite
=====

gokvlite is a simple key/value store written in go that implements
a single file key/value store for use as a library. It is meant to 
be embedded into another project rather than as a standalone 
database server. Its inspiration was primarily sqlite (with a near memcache api)

-----
Installation:
-----

> go get github.com/finder/gokvlite

-----
Usage
-----

Sample usage::

        package main

        import (
                "github.com/finder/gokvlite"
        )

        func main() {
                //Opens the file, doesn't matter if it exists or not.
                kh, err := gokvlite.Open("/tmp/kvlite.csv")
                if err != nil {
                        panic(fmt.Sprintf("Error:", err))
                }

                //Closes the file
                defer kh.Close()

                //Sets a key and checks for errors
                if err = kh.Set("Testing", []byte("This is the data")); err != nil {
                        panic(fmt.Sprintf("Error:", err))
                }

                //Gets the key and checks for errors
                data, err := kh.Get("Testing")
                if err != nil {
                        panic(fmt.Sprintf("Error:", err))
                }

                //Print out the data
                fmt.Println(string(*data))
        }


-------
Exports
-------

Types::

    type KeyHandler struct {
        // contains filtered or unexported fields
    }
        This struct actually sets/gets/deletes a key from the database

    func Open(filename string) (*KeyHandler, error)
        Opens a file to be used as a database. If the file doesn't exist, it'll
        create it and initialize it.

    func (kh *KeyHandler) Close() error
        Closes the file returned by Open, can be deferred that way

    func (kh *KeyHandler) Del(key string) error
        Deletes the key if it exists. (returns if it doesn't)

    func (kh *KeyHandler) Get(key string) (*[]byte, error)
        Gets the data contained at string Returns nil if the key doesn't exist

    func (kh *KeyHandler) Set(key string, data []byte) error
        Sets the key to data
