CONFIG_PATH=${HOME}/.goutube/

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONY: test
test:
	go test -v ./...

.PHONY: compile
compile:
	protoc api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.