package ketama

import (
	"context"
	"fmt"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ketama "github.com/dgryski/go-ketama"
)

func TestOldCompat(t *testing.T) {
	var oldBuckets []ketama.Bucket
	var newBuckets []bucket

	oldBuckets = append(oldBuckets, ketama.Bucket{"127.0.0.1", 1})
	oldBuckets = append(oldBuckets, ketama.Bucket{"127.0.0.1:11212", 1})
	oldBuckets = append(oldBuckets, ketama.Bucket{"127.0.0.1:11213", 1})

	newBuckets = append(newBuckets, bucket{"127.0.0.1", "foo", 1})
	newBuckets = append(newBuckets, bucket{"127.0.0.1:11212", "bar", 1})
	newBuckets = append(newBuckets, bucket{"127.0.0.1:11213", "baz", 1})

	oldC, err := ketama.New(oldBuckets)
	if err != nil {
		t.Fatalf("Cannot create old continuum: %s", err)
	}

	newC, err := newContinuum(newBuckets)
	if err != nil {
		panic(err)
	}

	tests := []string{
		"test-key-1",
		"test-key-2",
		"test-key-3",
		"test-key-4",
		"test-key-5",
		"test-key-6",
		"test-key-7",
		"test-key-8",
		"test-key-9",
		"test-key-0",
		"test-key-a",
		"test-key-b",
		"test-key-c",
		"test-key-d",
		"test-key-e",
		"test-key-f",
	}
	for _, test := range tests {
		t.Logf("Testing: %s", test)

		oldMap := oldC.Hash(test)
		newMap := newC.hash(test)

		idx := 0
		for ; idx < len(oldBuckets); idx++ {
			if oldMap == oldBuckets[idx].Label {
				break
			}

		}
		if idx == len(oldBuckets) {
			t.Errorf("Bucket not found")
			break
		}

		if newBuckets[idx].Label != oldBuckets[idx].Label {
			t.Errorf("Bucket labels do not match")
		}

		if *newMap != newBuckets[idx] {
			t.Errorf("Did not return correct bucket")
		}
	}
}

func TestTCPUDPCannotCoexist(t *testing.T) {
	tcp, err := net.ResolveTCPAddr("tcp", "127.0.0.1:1")
	if err != nil {
		panic(err)
	}
	udp, err := net.ResolveUDPAddr("udp", "127.0.0.1:1")
	if err != nil {
		panic(err)
	}

	servers := []Server{
		{tcp, 1},
		{udp, 1},
	}

	k := &Ketama{}
	err = k.SetServers(servers)
	if err == nil {
		t.Errorf("TCP and UDP cannot coexist.")
	}
}

func max(a int, b int) float64 {
	return math.Max(float64(a), float64(b))
}
func min(a int, b int) float64 {
	return math.Min(float64(a), float64(b))
}
func abs(a int) float64 {
	return math.Abs(float64(a))
}

func TestIfConsistent(t *testing.T) {
	buckets := []bucket{
		{"104.65.37.209", 0, 1},
		{"82.71.42.148", 1, 1},
		{"189.135.217.197", 2, 1},
		{"98.151.200.82", 3, 1},
		{"47.40.170.121", 4, 1},
		{"225.190.186.124", 5, 1},
		{"65.20.43.107", 6, 1},
		{"211.229.190.56", 7, 1},
		{"200.15.209.41", 8, 1},
		{"13.214.127.162", 9, 1},
	}

	dataCnt := 128 * 1024

	data := make([]string, 0, dataCnt)
	for i := 0; i < dataCnt; i++ {
		data = append(data, fmt.Sprintf("%08x", i))
	}

	type mapping struct {
		kb map[string]string
		bk map[string]int
	}

	fillMapping := func(buckets []bucket) mapping {
		mapping := mapping{
			kb: make(map[string]string),
			bk: make(map[string]int),
		}

		c, err := newContinuum(buckets)
		if err != nil {
			panic(err)
		}

		for _, d := range data {
			label := c.hash(d).Label
			mapping.kb[d] = label
			mapping.bk[label] += 1
		}

		return mapping
	}

	checkSpread := func(m mapping, n int) {
		total := len(data)
		ideal := float64(len(data)) / float64(n)
		// NOTE: Needed tolerance of 25% is higher than I would expect.
		//       I should investigate this closer when I have the time.
		tol := 0.25

		lbound := ideal * (1 - tol)
		rbound := ideal * (1 + tol)

		t.Logf("Checking %d buckets with tolerance %g%%:\n"+
			"total : %d\n"+
			"ideal : %g\n"+
			"lbound: %g\n"+
			"rbound: %g",
			n, tol*100,
			total,
			ideal,
			lbound,
			rbound,
		)

		if len(m.bk) < n {
			t.Errorf("Not enough in bk: %d instead of %d",
				len(m.bk), n)
			return
		}

		sum := 0
		devSum := float64(0)
		devMax := float64(0)
		for label, items := range m.bk {
			deviation := float64(items)/ideal - 1
			if float64(items) < lbound {
				t.Errorf("Not enough items (%d) in %s: %.4g%%",
					items, label, deviation*100)
			}
			if float64(items) > rbound {
				t.Errorf("Too many items (%d) in %s: %.4g%%",
					items, label, deviation*100)
			}

			sum += items

			dev := math.Abs(deviation)
			devSum += dev
			if dev > devMax {
				devMax = dev
			}
		}

		t.Logf("Average deviation: %.4g%%", devSum/float64(n)*100)
		t.Logf("Max deviation    : %.4g%%", devMax*100)

		if sum != total {
			t.Errorf("Sum of %d != expected total of %d",
				sum, total)
		}

		t.Log()
	}

	for i := 1; i <= 10; i++ {
		m := fillMapping(buckets[:i])
		checkSpread(m, i)
	}

	checkCacheMiss := func(i int, j int) {
		if i == j {
			panic("bug: i == j")
		}

		// Tolerance of actual cache miss versus ideal cache miss.
		// In absolute percents.
		tol := 0.05

		t.Logf("Checking cache miss between %d and %d servers", i, j)

		mi := fillMapping(buckets[:i])
		mj := fillMapping(buckets[:j])

		cacheMissCnt := 0
		for _, d := range data {
			if mi.kb[d] != mj.kb[d] {
				cacheMissCnt++
			}
		}

		idealMiss := 1.0 / max(i, j) * abs(i-j)
		cacheMiss := float64(cacheMissCnt) / float64(len(data))

		t.Logf("Ideal cache miss : %.4g%%", idealMiss*100)
		t.Logf("Actual cache miss: %.4g%%", cacheMiss*100)

		lbound := idealMiss - tol
		rbound := idealMiss + tol

		if cacheMiss < lbound || cacheMiss > rbound {
			t.Errorf("Cache miss is outside acceptable bounds.")
		}

		t.Log()
	}

	for i := 1; i <= 9; i++ {
		j := i + 1
		checkCacheMiss(i, j)
	}
	checkCacheMiss(1, 3)
	checkCacheMiss(3, 1)
	checkCacheMiss(5, 10)
	checkCacheMiss(10, 5)
	checkCacheMiss(1, 10)
	checkCacheMiss(10, 1)
}

func TestIfThreadSafe(t *testing.T) {
	k := &Ketama{}
	wg := sync.WaitGroup{}
	ctx, _ := context.WithTimeout(
		context.Background(),
		100*time.Millisecond,
	)

	const nWorkers = 3

	reads := [nWorkers]uint64{}
	writes := [nWorkers]uint64{}

	for i := 0; i < nWorkers; i++ {
		// reader
		wg.Add(1)
		go func(i int) {
			for ctx.Err() == nil {
				k.PickServer("some-key")
				atomic.AddUint64(&reads[i], 1)
			}
			wg.Done()
		}(i)
		// writer
		wg.Add(1)
		go func(i int) {
			for ctx.Err() == nil {
				addr := &net.TCPAddr{
					IP:   net.ParseIP("127.0.0.1"),
					Port: 11211,
				}
				k.SetServersAddr([]net.Addr{addr})
				atomic.AddUint64(&writes[i], 1)
			}
			wg.Done()
		}(i)
	}

	wg.Wait()

	t.Logf("Reads : %v", reads)
	t.Logf("Writes: %v", writes)
}

func BenchmarkPickServer(b *testing.B) {
	k := &Ketama{}
	k.SetServersAddr([]net.Addr{
		&net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 11211,
		},
	})

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		k.PickServer("some-key")
	}

	b.ReportAllocs()
}

func BenchmarkPickServerParallel(b *testing.B) {
	k := &Ketama{}
	k.SetServersAddr([]net.Addr{
		&net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: 11211,
		},
	})

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			k.PickServer("some-key")
		}
	})

	b.ReportAllocs()
}
