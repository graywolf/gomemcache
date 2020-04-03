package ketama

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/bradfitz/gomemcache/memcache"
)

// Server holds details about single server.
type Server struct {
	// Addr of the server. net.TCPAddr, net.UDPAddr and net.UnixAddr are
	// supported types.
	Addr   net.Addr
	// Weight this server should have. Must be >= 0. To mirror
	// libmemcached's behavior, 0 is considered same as 1.
	Weight int
}

// Ketama provides ketama-based server list. It is core stucture of this
// package.
type Ketama struct {
	addrs     []net.Addr
	continuum *continuum
	m         sync.RWMutex
}

// SetServers updates current list of server to servers. It is safe to call from
// multiple goroutines at once.
func (k *Ketama) SetServers(servers []Server) error {
	c, addrs, err := newContinuumFromServer(servers)
	if err != nil {
		return err
	}

	k.m.Lock()

	k.continuum = c
	k.addrs = addrs

	k.m.Unlock()

	return nil
}

// SetServers updates current list of server to addrs. All addresses have
// weight of 1. It is safe to call from multiple goroutines at once.
func (k *Ketama) SetServersAddr(addrs []net.Addr) error {
	servers := []Server{}
	for _, addr := range addrs {
		servers = append(servers, Server{Addr: addr})
	}

	return k.SetServers(servers)
}

// PickServer returns address onto which the key should go. Matches libmemcached
// in it's selection (that is whole point of this package). Safe to call from
// multiple goroutines at once.
func (k *Ketama) PickServer(key string) (net.Addr, error) {
	k.m.RLock()
	defer k.m.RUnlock()

	if k.continuum == nil {
		return nil, memcache.ErrNoServers
	}

	b := k.continuum.hash(key)
	return b.UserData.(net.Addr), nil
}

// Each calls fn with every address that is currently registered into this
// server list.
func (k *Ketama) Each(fn func(net.Addr) error) error {
	k.m.RLock()
	addrs := k.addrs
	k.m.RUnlock()

	for _, addr := range addrs {
		err := fn(addr)
		if err != nil {
			return err
		}
	}

	return nil
}

func newContinuumFromServer(
	servers []Server,
) (
	c *continuum,
	addrs []net.Addr,
	err error,
) {
	var seenTypes int
	var buckets []bucket
	var label string

	if len(servers) == 0 {
		return
	}

	for _, server := range servers {
		label, err = addr2label(server.Addr, &seenTypes)
		if err != nil {
			return
		}

		addrs = append(addrs, server.Addr)
		buckets = append(buckets, bucket{
			Label:    label,
			UserData: server.Addr,
			Weight:   server.Weight,
		})
	}

	if seenTypes&typeTCP != 0 && seenTypes&typeUDP != 0 {
		err = errors.New("TCP and UDP connection cannot coexist")
		return
	}

	c, err = newContinuum(buckets)
	return
}

func maybePort(port int) string {
	switch port {
	case 11211:
		return ""
	default:
		return fmt.Sprintf(":%d", port)
	}
}

func addr2label(addr net.Addr, seenTypes *int) (string, error) {
	switch a := addr.(type) {
	case *net.TCPAddr:
		*seenTypes |= typeTCP
		return fmt.Sprintf("%s%s", a.IP, maybePort(a.Port)), nil
	case *net.UDPAddr:
		*seenTypes |= typeUDP
		return fmt.Sprintf("%s%s", a.IP, maybePort(a.Port)), nil
	case *net.UnixAddr:
		*seenTypes |= typeUnix
		return a.Name + ":0", nil
	default:
		return "", fmt.Errorf("Unsupported type: %T (%q)", addr, addr)
	}
}

const (
	typeTCP = 1 << iota
	typeUDP
	typeUnix
)
