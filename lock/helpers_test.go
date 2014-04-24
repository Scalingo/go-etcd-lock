package lock

import (
	"log"
	"net"

	"github.com/coreos/go-etcd/etcd"
)

func init() {
	s, err := net.Dial("tcp", "127.0.0.1:4001")
	if err != nil {
		log.Fatalln("etcd is not running on localhost", err)
	}
	s.Close()
}

func client() *etcd.Client {
	return etcd.NewClient([]string{"http://localhost:4001"})
}
