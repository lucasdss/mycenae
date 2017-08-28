package meta

import (
	"strings"

	"github.com/uol/gobol"
	pb "github.com/uol/mycenae/lib/proto"
	"github.com/uol/mycenae/lib/structs"
	index "github.com/uol/mycenae/lib/ts-index"
	"go.uber.org/zap"
)

type localMeta struct {
	index  *index.Set
	logger *zap.Logger
}

func createLocal(logger *zap.Logger) *localMeta {
	return &localMeta{
		index:  index.CreateSet(),
		logger: logger,
	}
}

// NewLocal creates a local file meta store
func NewLocal(logger *zap.Logger) Backend {
	return createLocal(logger)
}

func (m *localMeta) Handle(pkt *pb.Meta) bool {
	m.logger.Debug("Handling package",
		zap.String("function", "Handle"),
		zap.String("structurte", "localMeta"),
		zap.String("package", packageName),
	)
	lindex := m.index.Get(pkt.GetKsid(), elasticMetaType)
	if lindex == nil {
		m.CreateIndex(pkt.GetKsid())
		lindex = m.index.Get(pkt.GetKsid(), elasticMetaType)
		m.logger.Debug("Creating index",
			zap.String("index", pkt.GetKsid()),
			zap.String("function", "Handle"),
			zap.String("structurte", "localMeta"),
			zap.String("package", packageName),
		)
	}
	found, err := lindex.Exists(index.ParseID(pkt.GetTsid()))
	if err != nil {
		m.logger.Debug("Error getting index",
			zap.String("function", "Handle"),
			zap.String("structurte", "localMeta"),
			zap.String("package", packageName),
		)
	}
	if !found {
		err = lindex.Add(index.Metric(pkt.GetMetric()), index.ParseTags(pkt.GetTags()), index.ParseID(pkt.GetTsid()))
		if err != nil {
			m.logger.Debug("Error indexing timeseries",
				zap.String("function", "Handle"),
				zap.String("structurte", "localMeta"),
				zap.String("package", packageName),
			)
		}
	}
	return (err == nil && !found)
}

func (m *localMeta) SaveTxtMeta(packet *pb.Meta) {
	var (
		keyspace   = packet.GetKsid()
		timeseries = index.ParseID(packet.GetTsid())
		metric     = index.Metric(packet.GetMetric())

		tags []index.KVPair
	)
	for _, tag := range packet.GetTags() {
		tags = append(tags, index.KVPair{Key: tag.GetKey(), Value: tag.GetValue()})
	}
	m.index.Get(keyspace, "metatext").Add(metric, tags, timeseries)
}

func (m *localMeta) CheckTSID(dtype, id string) (bool, gobol.Error) {
	info := strings.Split(id, "|")
	esindex, id := info[0], info[1]
	exists, err := m.index.Get(esindex, dtype).Exists(index.ParseID(id))
	if err != nil {
		return false, errPersist("Exists", err)
	}
	return exists, nil
}

func (m *localMeta) SendError(index, dtype, id string, doc ErrorData) gobol.Error {
	return errNotImplemented("SendError", "localMeta")
}

func (m *localMeta) ListTags(keyspace, dtype, tagkey string, size, from int64) ([]string, int, gobol.Error) {
	return nil, 0, errNotImplemented("ListTags", "localMeta")
}

func (m *localMeta) ListMetrics(keyspace, esType, metricName string, size, from int64) ([]string, int, gobol.Error) {
	return nil, 0, errNotImplemented("ListMetrics", "localMeta")
}

func (m *localMeta) ListTagKey(keyspace, tagKname string, size, from int64) ([]string, int, gobol.Error) {
	return nil, 0, errNotImplemented("ListTagKey", "localMeta")
}

func (m *localMeta) ListTagValue(keyspace, tagVname string, size, from int64) ([]string, int, gobol.Error) {
	return nil, 0, errNotImplemented("ListTagValue", "localMeta")
}

func (m *localMeta) ListMeta(
	keyspace, esType, metric string, tags map[string]string,
	onlyids bool, size, from int64,
) ([]TSInfo, int, gobol.Error) {
	return nil, 0, errNotImplemented("ListMeta", "localMeta")
}

func (m *localMeta) MetaOpenTSDB(
	keyspace, id, metric string, tags map[string][]string,
	size, from int64,
) ([]TSDBData, int, gobol.Error) {
	return nil, 0, errNotImplemented("MetaOpenTSDB", "localMeta")
}

func (m *localMeta) MetaFilterOpenTSDB(
	keyspace, id, metric string,
	filters []structs.TSDBfilter, size int64,
) ([]TSDBData, int, gobol.Error) {
	return nil, 0, errNotImplemented("MetaFilterOpenTSDB", "localMeta")
}

func (m *localMeta) ListErrorTags(
	keyspace, esType, metric string,
	tags []Tag, size, from int64,
) ([]string, int, gobol.Error) {
	return nil, 0, errNotImplemented("ListErrorTags", "localMeta")
}

func (m *localMeta) CreateIndex(name string) gobol.Error {
	m.index.Add(name, elasticMetaType, index.Create())
	return nil
}

func (m *localMeta) DeleteIndex(index string) gobol.Error {
	m.index.Delete(index, elasticMetaType)
	return nil
}
