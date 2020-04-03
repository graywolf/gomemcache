check: ;

check: check-cgo
check: check-cgo-race
check: check-nocgo
check: check-serverlist-ketama

check-cgo: FORCE
	CGO_ENABLED=1 go test -v -bench . ./...

check-cgo-race: FORCE
	CGO_ENABLED=1 go test -race -v -bench . ./...

check-nocgo: FORCE
	CGO_ENABLED=0 go test -v -bench . ./...

KETAMA_C  = serverlist/ketama/tests/test-c
KETAMA_GO = serverlist/ketama/tests/test-go

check-serverlist-ketama: $(KETAMA_C)
check-serverlist-ketama: $(KETAMA_GO)
check-serverlist-ketama:
	./serverlist/ketama/tests/run-test

$(KETAMA_C): $(KETAMA_C).c
	gcc -o $@ -g -O2 $(@:=.c) -lmemcached

$(KETAMA_GO): $(KETAMA_GO).go
	go build -buildmode=exe -o $@ $(@:=.go)

clean:
	rm -f $(KETAMA_C)
	rm -f $(KETAMA_GO)
	rm -f serverlist/ketama/tests/data
	rm -rf serverlist/ketama/tests/logs
	rm -rf serverlist/ketama/tests/pids
	awk '/^u / {print $$2}' serverlist/ketama/tests/servers | xargs rm -fv

FORCE: ;
