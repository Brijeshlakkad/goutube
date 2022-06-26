CONFIG_PATH=${HOME}/.goutube/

.PHONY: init
init:
	mkdir -p ${CONFIG_PATH}

.PHONY: test
test:
	go test -race ./...
