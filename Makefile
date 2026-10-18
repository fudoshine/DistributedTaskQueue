BUF_VERSION := v1.40.0
BUF := go run github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)
PROTO_BREAKING_AGAINST ?=

.PHONY: proto-check proto-breaking

# Validates the checked-in worker wire contract without writing generated code.
proto-check:
	$(BUF) lint api/proto
	$(BUF) build api/proto

# Compares the current contract with an explicit prior contract source.
proto-breaking:
	test -n "$(PROTO_BREAKING_AGAINST)"
	$(BUF) breaking api/proto --against "$(PROTO_BREAKING_AGAINST)"
