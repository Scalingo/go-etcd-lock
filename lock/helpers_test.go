package lock

import (
	"log"
	"net"

	etcdv3 "go.etcd.io/etcd/client/v3"
)

func init() {
	s, err := net.Dial("tcp", "127.0.0.1:2379")
	if err != nil {
		log.Fatalln("etcd is not running on localhost", err)
	}
	s.Close()
}

func client() *etcdv3.Client {
	client, err := etcdv3.New(etcdv3.Config{Endpoints: []string{"http://localhost:2379"}})
	if err != nil {
		panic(err)
	}
	return client
}
