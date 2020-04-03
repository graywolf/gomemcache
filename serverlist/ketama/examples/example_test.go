package examples

import (
	"fmt"
	"net"

	"github.com/bradfitz/gomemcache/memcache"

	"git.sr.ht/~graywolf/gomemcache/serverlist/ketama"
)

func ExampleUsage() {
	k := &ketama.Ketama{}
	k.SetServersAddr([]net.Addr{&net.TCPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 1,
	}})

	mc := memcache.NewFromSelector(k)
	fmt.Println(mc.Get("some-key"))

	// Output:
	// <nil> dial tcp 127.0.0.1:1: connect: connection refused
}
