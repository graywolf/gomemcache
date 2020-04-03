// +build test

package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"

	"git.sr.ht/~graywolf/gomemcache/serverlist/ketama"
)

func die(_fmt string, _data ...interface{}) {
	fmt.Fprintf(os.Stderr, _fmt, _data...)
	fmt.Fprintf(os.Stderr, "\n")
	os.Exit(1)
}

var k *ketama.Ketama
var addrs []net.Addr
var mc *memcache.Client

func processServerLine(line string) {
	s := bufio.NewScanner(strings.NewReader(line))
	s.Split(bufio.ScanWords)

	parts := make([]string, 0, 3)

	for s.Scan() {
		parts = append(parts, s.Text())
	}
	if err := s.Err(); err != nil {
		die("Cannot scan server line: %s", err)
	}

	var addr net.Addr
	var err error

	switch parts[0] {
	case "t":
		if len(parts) != 3 {
			die("Incorrect len(parts): %q", parts)
		}
		ip := parts[1]
		port := parts[2]

		if strings.Contains(ip, ":") {
			ip = "[" + ip + "]"
		}

		addr, err = net.ResolveTCPAddr("tcp", ip+":"+port)
		if err != nil {
			die("Cannot resolve tcp addr: %q", parts)
		}
	case "u":
		if len(parts) != 2 {
			die("Incorrect len(parts): %q", parts)
		}
		path := parts[1]

		addr, err = net.ResolveUnixAddr("unix", path)
		if err != nil {
			die("Cannot resolve unix addr: %q", parts)
		}
	default:
		die("Unknown server type: %s", parts[0])
	}

	addrs = append(addrs, addr)
}

func main() {
	if len(os.Args) != 3 {
		die("Usage: test-go SERVERS DATA")
	}

	servers := os.Args[1]
	data := os.Args[2]

	k := &ketama.Ketama{}

	sf, err := os.Open(servers)
	if err != nil {
		die("Cannot open servers file: %s", err)
	}
	defer sf.Close()

	df, err := os.Open(data)
	if err != nil {
		die("Cannot open data file: %s", err)
	}
	defer df.Close()

	ss := bufio.NewScanner(sf)
	for ss.Scan() {
		processServerLine(ss.Text())
	}
	if err := ss.Err(); err != nil {
		die("Scanning servers file failed: %s", err)
	}

	err = k.SetServersAddr(addrs)
	if err != nil {
		die("Cannot SetServersAddr: %s", err)
	}

	ds := bufio.NewScanner(df)
	for ds.Scan() {
		mc = memcache.NewFromSelector(k)
		err := mc.Set(&memcache.Item{
			Key:   ds.Text(),
			Value: []byte("value :-> " + ds.Text()),
		})
		if err != nil {
			die("Failed to set %q: %s", ds.Text(), err)
		}
	}
	if err := ds.Err(); err != nil {
		die("Scanning data file failed: %s", err)
	}
}
