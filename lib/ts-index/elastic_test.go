package index

import "go.uber.org/zap"

var _ Backend = &esIndex{}

func testLogger() *zap.Logger {
	return zap.NewNop()
}

func testEsClient() (*esIndex, error) {
	return createESIndex(esHost, index, "meta", testLogger())
}
