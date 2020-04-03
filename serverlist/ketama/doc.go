/*
Package ketama provides libmemcached-compatible consistent hashing. That means
that you can use memcached from go as well as from any libmemcached based
library and have same keys go to the same nodes in your cluster.

It is designed with github.com/bradfitz/gomemcache/memcache package in mind, by
implementing memcache.ServerSelector interface it is drop-in replacement for
memcache.ServerList.

Usage could look something like this:

	k := &ketama.Ketama{}
	k.SetServersAddr([]net.Addr{&net.TCPAddr{
		IP: net.ParseIP("127.0.0.1"),
		Port: 11211,
	}})

	mc := memcache.NewFromSelector(k)
	fmt.Println(mc.Get("some-key"))
*/
package ketama
