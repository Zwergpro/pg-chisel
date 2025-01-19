.PHONY: gocyclo test


gocyclo:
	gocyclo .


test:
	go clean -testcache
	go test -race -coverprofile=coverage.out ./...
	grep -v mocks coverage.out > coverage_no_mocks.out
	go tool cover -func=coverage_no_mocks.out
	rm coverage.out coverage_no_mocks.out
