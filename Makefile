args = `arg="$(filter-out $@,$(MAKECMDGOALS))" && echo $${arg:-${1}}`

.PHONY: build run test

build:
	./scripts/build.sh $(call args)

run:
	./scripts/run.sh $(call args)

test:
	go test ./api/... ./internal/...

