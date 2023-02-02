start:
	@go run -race main.go


test:
	@go test -v -race -cpuprofile cpu.out -memprofile mem.out -coverprofile cover.out
	@go tool cover -html cover.out -o cover.html
	@open cover.html
