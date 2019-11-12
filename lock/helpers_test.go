package lock

import (
	"log"
	"net"

	etcd "go.etcd.io/etcd/clientv3"
)

func init() {
	s, err := net.Dial("tcp", "127.0.0.1:2379")
	if err != nil {
		log.Fatalln("etcd is not running on localhost", err)
	}
	s.Close()
}

func client() *etcd.Client {
	client, err := etcd.New(etcd.Config{Endpoints: []string{"http://localhost:2379"}})
	if err != nil {
		panic(err)
	}
	return client
}
