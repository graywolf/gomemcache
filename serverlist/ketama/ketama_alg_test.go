package ketama

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"
)

func TestBasicCompat(t *testing.T) {

	var compatTest = []bucket{
		{"server1", nil, 8699},
		{"server10", nil, 9462},
		{"server2", nil, 10885},
		{"server3", nil, 9980},
		{"server4", nil, 10237},
		{"server5", nil, 9099},
		{"server6", nil, 10997},
		{"server7", nil, 10365},
		{"server8", nil, 10380},
		{"server9", nil, 9896},
	}

	var buckets []bucket

	for i := 1; i <= 10; i++ {
		b := &bucket{Label: fmt.Sprintf("server%d", i), Weight: 1}
		buckets = append(buckets, *b)
	}

	k, err := newContinuum(buckets)
	if err != nil {
		panic(err)
	}

	m := make(map[string]int)

	for i := 0; i < 100000; i++ {
		s := k.hash("foo" + strconv.Itoa(i))
		m[s.Label]++
	}

	for _, tt := range compatTest {
		if m[tt.Label] != tt.Weight {
			t.Errorf("basic compatibility check failed "+
				"key=%s expected=%d got=%d",
				tt.Label, tt.Weight, m[tt.Label])
		}
	}
}

func TestSegfault(t *testing.T) {

	// perl -Mblib -MAlgorithm::ConsistentHash::Ketama \
	//	-wE ' \
	//		my $ketama = Algorithm::ConsistentHash::Ketama->new(); \
	//		$ketama->add_bucket( "r01", 100 ); \
	//		$ketama->add_bucket( "r02", 100 ); \
	//		my $key = $ketama->hash( \
	//			pack "H*", "37292b669dd8f7c952cf79ca0dc6c5d7" \
	//		); \
	//		say $key \
	//	'

	buckets := []bucket{
		bucket{Label: "r01", Weight: 100},
		bucket{Label: "r02", Weight: 100},
	}
	k, err := newContinuum(buckets)
	if err != nil {
		panic(err)
	}

	tests := []struct {
		key string
		b   string
	}{
		{"161c6d14dae73a874ac0aa0017fb8340", "r01"},
		{"37292b669dd8f7c952cf79ca0dc6c5d7", "r01"},
	}

	for _, tt := range tests {
		key, _ := hex.DecodeString(tt.key)
		b := k.hash(string(key))
		if b.Label != tt.b {
			t.Errorf("k.Hash(%v)=%v, want %v", tt.key, b, tt.b)
		}
	}

}

func TestAcceptableWeights(t *testing.T) {
	var err error
	var c *continuum

	_, err = newContinuum([]bucket{{"foo", nil, 1}})
	if err != nil {
		t.Errorf("Weight of 1 must be supported.")
	}

	c, err = newContinuum([]bucket{{"foo", nil, 0}})
	if err != nil {
		t.Errorf("Weight of 0 must be supported.")
	}
	if len(c.ring) == 0 {
		t.Errorf("Weight of 0 must be considered to be 1.")
	}

	_, err = newContinuum([]bucket{{"foo", nil, -1}})
	if err == nil {
		t.Errorf("Weight < 0 must be rejected.")
	}
	if err != ErrNegativeWeight {
		t.Errorf("Wrong error returned.")
	}
}
