package ketama

// Source: github.com/dgryski/go-ketama
// Patched: by me
//
// Package ketama implements consistent hashing compatible with
// Algorithm::ConsistentHash::Ketama
/*
This implementation draws from the Daisuke Maki's Perl module, which itself is
based on the original libketama code.  That code was licensed under the GPLv2,
and thus so is this.

The major API change from libketama is that Algorithm::ConsistentHash::Ketama
allows hashing arbitrary strings, instead of just memcached server IP addresses.
*/

import (
	"crypto/md5"
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	ErrNegativeWeight = errors.New("negative weight is not allowed")
)

type bucket struct {
	Label    string
	UserData interface{}
	Weight   int
}

type continuumPoint struct {
	bucket bucket
	point  uint
}

type continuum struct {
	ring points
}

type points []continuumPoint

func (c points) Less(i, j int) bool { return c[i].point < c[j].point }
func (c points) Len() int           { return len(c) }
func (c points) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

var bufPool = sync.Pool{
	New: func() interface{} {
		// Key is allowed to be at max 250 characters. We do not really
		// care if it is sliced (and hashed wrong) since *any* memcached
		// will reject such key anyway.
		//
		// Reserved value of 256 is actually by few nanoseconds faster
		// then 250, I did not really dig into why.
		b := make([]byte, 256)
		return &b
	},
}

func md5Digest(in string) [md5.Size]byte {
	buf := bufPool.Get().(*[]byte)

	n := copy(*buf, in)
	sum := md5.Sum((*buf)[:n])

	bufPool.Put(buf)

	return sum
}

func hashString(in string) uint {
	digest := md5Digest(in)
	return uint(digest[3])<<24 |
		uint(digest[2])<<16 |
		uint(digest[1])<<8 |
		uint(digest[0])
}

func fixWeight(weight int) int {
	// libmemcached treats 0 weight to be the same as 1 so we should behave
	// the same way
	if weight == 0 {
		return 1
	}

	return weight
}

func newContinuum(buckets []bucket) (*continuum, error) {

	numbuckets := len(buckets)

	if numbuckets == 0 {
		// let them error when they try to use it
		return nil, nil
	}

	ring := make(points, 0, numbuckets*160)

	totalweight := 0
	for _, b := range buckets {
		if b.Weight < 0 {
			return nil, ErrNegativeWeight
		}

		totalweight += fixWeight(b.Weight)
	}

	for i, b := range buckets {
		pct := float32(fixWeight(b.Weight)) / float32(totalweight)

		// this is the equivalent of C's promotion rules, but in Go, to
		// maintain exact compatibility with the C library
		limit := int(float32(float64(pct) * 40.0 * float64(numbuckets)))

		for k := 0; k < limit; k++ {
			/* 40 hashes, 4 numbers per hash = 160 points per
			* bucket */
			ss := fmt.Sprintf("%s-%d", b.Label, k)
			digest := md5Digest(ss)

			for h := 0; h < 4; h++ {
				point := continuumPoint{
					point: uint(digest[3+h*4])<<24 |
						uint(digest[2+h*4])<<16 |
						uint(digest[1+h*4])<<8 |
						uint(digest[h*4]),
					bucket: buckets[i],
				}
				ring = append(ring, point)
			}
		}
	}

	sort.Sort(ring)

	c := &continuum{ring: ring}
	return c, nil
}

func (c continuum) hash(thing string) *bucket {

	if len(c.ring) == 0 {
		return nil
	}

	h := hashString(thing)
	i := search(c.ring, h)

	return &c.ring[i].bucket
}

// This function taken from
// https://github.com/lestrrat/Algorithm-ConsistentHash-Ketama/blob/master/xs/Ketama.xs
// In order to maintain compatibility, we must reproduce the same integer
// underflow bug introduced in
// https://github.com/lestrrat/Algorithm-ConsistentHash-Ketama/commit/1efbcc0ead13114f8e4e454a8064b842b14da6f3

func search(ring points, h uint) uint {
	var maxp = uint(len(ring))
	var lowp = uint(0)
	var highp = maxp

	for {
		midp := (lowp + highp) / 2
		if midp >= maxp {
			if midp == maxp {
				midp = 1
			} else {
				midp = maxp
			}

			return midp - 1
		}
		midval := ring[midp].point
		var midval1 uint
		if midp == 0 {
			midval1 = 0
		} else {
			midval1 = ring[midp-1].point
		}

		if h <= midval && h > midval1 {
			return midp
		}

		if midval < h {
			lowp = midp + 1
		} else {
			// NOTE(dgryski): Maintaining compatibility with
			// Algorithm::ConsistentHash::Ketama depends on integer
			// underflow here
			highp = midp - 1
		}

		if lowp > highp {
			return 0
		}
	}
}
