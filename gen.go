package agentsandbox

// `make proto` and `make mocks` run buf and mockery with the binaries that
// `make install-tools-gen` places on PATH; go.mod's `tool` directives pin them.

//go:generate buf generate
//go:generate go tool mockery
