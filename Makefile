start:
	@go run -race main.go


test:
	@go test -v -race -cpuprofile cpu.out -memprofile mem.out
