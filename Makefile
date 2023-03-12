build:
	go build
run:
	go build
	./csvdb  "/home/j/test.csv" "select Year limit 3"
clean:
	rm csvdb
test:
	go test ./...