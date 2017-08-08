package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

var errKsName = "Wrong Format: Field \"name\" is not well formed. NO information will be saved"
var errKsRF = "Replication factor can not be less than or equal to 0 or greater than 3"
var errKsDCNil = "Datacenter can not be empty or nil"
var errKsDC = "Cannot create because datacenter \"dc_error\" not exists"
var errKsContact = "Contact field should be a valid email address"
var errKsTTL = "TTL can not be less or equal to zero"

func getKeyspace() tools.Keyspace {

	data := tools.Keyspace{
		Name:              getRandName(),
		Datacenter:        "datacenter1",
		ReplicationFactor: 1,
		Contact:           fmt.Sprintf("test-%d@domain.com", time.Now().Unix()),
		TTL:               90,
	}

	return data
}

func getRandName() string {
	rand.Seed(time.Now().UnixNano())
	return fmt.Sprintf("%d", rand.Int())
}

func testKeyspaceCreation(data *tools.Keyspace, t *testing.T) {

	body, err := json.Marshal(data)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	path := fmt.Sprintf("keyspaces/%s", data.Name)
	code, resp, err := mycenaeTools.HTTP.POST(path, body)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}
	assert.Equal(t, 201, code)

	var ksr tools.KeyspaceResp
	err = json.Unmarshal(resp, &ksr)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	data.ID = ksr.KSID
	assert.Contains(t, data.ID, "ts_", "Keyspace ID was poorly formed")
	assert.Equal(t, 1, mycenaeTools.Cassandra.Timeseries.CountTsKeyspaceByName(data.Name))
	assert.True(t, mycenaeTools.Cassandra.Timeseries.Exists(data.ID), fmt.Sprintf("Keyspace %v was not created", data.ID))
	assert.True(t, mycenaeTools.Cassandra.Timeseries.ExistsInformation(data.ID, data.Name, data.ReplicationFactor, data.Datacenter, data.TTL, false, data.Contact), "Keyspace information was not stored")
}

func testKeyspaceCreationFail(data []byte, keyName string, response tools.Error, t *testing.T) {

	path := fmt.Sprintf("keyspaces/%s", keyName)
	code, resp, err := mycenaeTools.HTTP.POST(path, data)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	var respErr tools.Error
	err = json.Unmarshal(resp, &respErr)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	assert.Equal(t, 400, code)
	assert.Equal(t, response, respErr)
	assert.Equal(t, 0, mycenaeTools.Cassandra.Timeseries.CountTsKeyspaceByName(keyName))
}

func testKeyspaceEdition(id string, data tools.KeyspaceEdit, t *testing.T) {

	ksBefore := mycenaeTools.Cassandra.Timeseries.KsAttributes(id)

	body, err := json.Marshal(data)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	path := fmt.Sprintf("keyspaces/%s", id)
	code, _, err := mycenaeTools.HTTP.PUT(path, body)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)

	ksAfter := mycenaeTools.Cassandra.Timeseries.KsAttributes(id)
	assert.True(t, ksBefore != ksAfter)
	assert.Equal(t, data.Name, ksAfter.Name)
	assert.Equal(t, data.Contact, ksAfter.Contact)
}

func testKeyspaceEditionConcurrently(id string, data1 tools.KeyspaceEdit, data2 tools.KeyspaceEdit, t *testing.T, wg *sync.WaitGroup) {

	ksBefore := mycenaeTools.Cassandra.Timeseries.KsAttributes(id)

	body, err := json.Marshal(data1)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	path := fmt.Sprintf("keyspaces/%s", id)
	code, _, err := mycenaeTools.HTTP.PUT(path, body)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)

	ksAfter := mycenaeTools.Cassandra.Timeseries.KsAttributes(id)
	assert.True(t, ksBefore != ksAfter)
	assert.True(t, data1.Name == ksAfter.Name || data2.Name == ksAfter.Name)
	assert.True(t, data1.Contact == ksAfter.Contact || data2.Contact == ksAfter.Contact)

	wg.Done()
}

func testKeyspaceEditionFail(id string, data []byte, status int, response tools.Error, t *testing.T) {

	ksBefore := mycenaeTools.Cassandra.Timeseries.KsAttributes(id)

	path := fmt.Sprintf("keyspaces/%s", id)
	code, resp, err := mycenaeTools.HTTP.PUT(path, data)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	var respErr tools.Error
	err = json.Unmarshal(resp, &respErr)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	ksAfter := mycenaeTools.Cassandra.Timeseries.KsAttributes(id)

	assert.Equal(t, status, code)
	assert.Equal(t, response, respErr)
	assert.True(t, ksBefore == ksAfter)
}

func checkTables(data tools.Keyspace, t *testing.T) {

	var tables = []string{"timeseries", "ts_text_stamp"}
	var elasticSearchIndexMeta = "\"meta\":{\"properties\":{\"tagsNested\":{\"type\":\"nested\",\"properties\":{\"tagKey\":{\"type\":\"string\"},\"tagValue\":{\"type\":\"string\"}}}}}"
	var elasticSearchIndexMetaText = "\"metatext\":{\"properties\":{\"tagsNested\":{\"type\":\"nested\",\"properties\":{\"tagKey\":{\"type\":\"string\"},\"tagValue\":{\"type\":\"string\"}}}}}"
	var caching = map[string]string{
		"keys":               "ALL",
		"rows_per_partition": "NONE",
	}
	var compaction = map[string]string{
		"max_threshold":          "64",
		"min_threshold":          "8",
		"class":                  "org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy",
		"compaction_window_size": "7",
		"compaction_window_unit": "DAYS",
	}
	var compression = map[string]string{
		"class":              "org.apache.cassandra.io.compress.LZ4Compressor",
		"chunk_length_in_kb": "64",
	}

	esIndexResponse := mycenaeTools.ElasticSearch.Keyspace.GetIndex(data.ID)
	tableProperties := mycenaeTools.Cassandra.Timeseries.TableProperties(data.ID, "timeseries")

	for _, table := range tables {
		tableProperties = mycenaeTools.Cassandra.Timeseries.TableProperties(data.ID, table)
		assert.Exactly(t, 0.01, tableProperties.Bloom_filter_fp_chance)
		assert.Exactly(t, caching, tableProperties.Caching)
		assert.Exactly(t, "", tableProperties.Comment)
		assert.Exactly(t, compaction, tableProperties.Compaction)
		assert.Exactly(t, compression, tableProperties.Compression)
		assert.Exactly(t, 0.0, tableProperties.Dclocal_read_repair_chance)
		assert.Exactly(t, data.TTL*86400, tableProperties.Default_time_to_live)
		assert.Exactly(t, 0, tableProperties.Gc_grace_seconds)
		assert.Exactly(t, 2048, tableProperties.Max_index_interval)
		assert.Exactly(t, 0, tableProperties.Memtable_flush_period_in_ms)
		assert.Exactly(t, 128, tableProperties.Min_index_interval)
		assert.Exactly(t, 0.0, tableProperties.Read_repair_chance)
		assert.Exactly(t, "99PERCENTILE", tableProperties.Speculative_retry)
	}

	assert.Contains(t, data.ID, "ts_", "We received a weird keyspace name for request")
	assert.True(t, mycenaeTools.Cassandra.Timeseries.Exists(data.ID), "Keyspace was not created")

	keyspaceCassandraTables := mycenaeTools.Cassandra.Timeseries.KeyspaceTables(data.ID)
	sort.Strings(keyspaceCassandraTables)
	sort.Strings(tables)
	assert.Equal(t, tables, keyspaceCassandraTables)
	assert.Contains(t, string(esIndexResponse), elasticSearchIndexMeta)
	assert.Contains(t, string(esIndexResponse), elasticSearchIndexMetaText)
}

// CREATE

func TestKeyspaceCreateFail(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var (
		dc      = "datacenter1"
		rf      = 1
		ttl     = 90
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	cases := map[tools.Error]tools.Keyspace{
		{Error: errKsDCNil, Message: errKsDCNil}:     {ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: getRandName()},
		{Error: errKsRF, Message: errKsRF}:           {Datacenter: dc, TTL: ttl, Contact: contact, Name: getRandName()},
		{Error: errKsTTL, Message: errKsTTL}:         {Datacenter: dc, ReplicationFactor: rf, Contact: contact, Name: getRandName()},
		{Error: errKsContact, Message: errKsContact}: {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Name: getRandName()},
		{Error: errKsName, Message: errKsName}:       {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: "test_*123"},
		{Error: errKsName, Message: errKsName}:       {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: "_test123"},
		{Error: errKsRF, Message: errKsRF}:           {Datacenter: dc, ReplicationFactor: 4, TTL: ttl, Contact: contact, Name: getRandName()},
		{Error: errKsRF, Message: errKsRF}:           {Datacenter: dc, ReplicationFactor: -1, TTL: ttl, Contact: contact, Name: getRandName()},
		{Error: errKsRF, Message: errKsRF}:           {Datacenter: dc, ReplicationFactor: 0, TTL: ttl, Contact: contact, Name: getRandName()},
		{Error: errKsDCNil, Message: errKsDCNil}:     {Datacenter: "", ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: getRandName()},
		{Error: errKsDC, Message: errKsDC}:           {Datacenter: "dc_error", ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: getRandName()},
		{Error: errKsTTL, Message: errKsTTL}:         {Datacenter: dc, ReplicationFactor: rf, TTL: 91, Contact: contact, Name: getRandName()},
		{Error: errKsTTL, Message: errKsTTL}:         {Datacenter: dc, ReplicationFactor: rf, TTL: -10, Contact: contact, Name: getRandName()},
		{Error: errKsTTL, Message: errKsTTL}:         {Datacenter: dc, ReplicationFactor: rf, TTL: 0, Contact: contact, Name: getRandName()},
		{Error: errKsContact, Message: errKsContact}: {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "test@test@test.com", Name: getRandName()},
		{Error: errKsContact, Message: errKsContact}: {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "test@testcom", Name: getRandName()},
		{Error: errKsContact, Message: errKsContact}: {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "testtest.com", Name: getRandName()},
		{Error: errKsContact, Message: errKsContact}: {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "@test.com", Name: getRandName()},
		{Error: errKsContact, Message: errKsContact}: {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "test@", Name: getRandName()},
		{Error: errKsContact, Message: errKsContact}: {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "test@t est.com", Name: getRandName()},
	}

	for err, ks := range cases {
		testKeyspaceCreationFail(ks.Marshal(), ks.Name, err, t)
	}
}

func TestKeyspaceCreateNewTimeseriesNewSuccessRF1(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	data.ReplicationFactor = 1
	testKeyspaceCreation(&data, t)
	checkTables(data, t)
}

func TestKeyspaceCreateNewTimeseriesNewSuccessRF2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	data.ReplicationFactor = 2
	testKeyspaceCreation(&data, t)
	checkTables(data, t)
}

func TestKeyspaceCreateNewTimeseriesNewSuccessRF3(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	data.ReplicationFactor = 3
	testKeyspaceCreation(&data, t)
	checkTables(data, t)
}

func TestKeyspaceCreateWithConflict(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	body, err := json.Marshal(data)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	path := fmt.Sprintf("keyspaces/%s", data.Name)
	code, resp, err := mycenaeTools.HTTP.POST(path, body)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	var respErr tools.Error
	err = json.Unmarshal(resp, &respErr)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	errConflict := tools.Error{
		Error:   "Cannot create because keyspace \"" + data.Name + "\" already exists",
		Message: "Cannot create because keyspace \"" + data.Name + "\" already exists",
	}

	assert.Equal(t, 409, code)
	assert.Equal(t, errConflict, respErr)
}

func TestKeyspaceCreateInvalidRFString(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := `{
		"datacenter": "datacenter1",
		"replicationFactor": "a",
		"ttl": 90,
		"tuuid": false,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `"
	}`

	var respErr = tools.Error{
		Error:   "json: cannot unmarshal string into Go struct field Config.replicationFactor of type int",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), getRandName(), respErr, t)
}

func TestKeyspaceCreateInvalidRFFloat(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	rf := "1.1"
	data := `{
		"datacenter": "datacenter1",
		"replicationFactor": ` + rf + `,
		"ttl": 90,
		"tuuid": false,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `"
	}`

	var respErr = tools.Error{
		Error:   "json: cannot unmarshal number " + rf + " into Go struct field Config.replicationFactor of type int",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), getRandName(), respErr, t)
}

func TestKeyspaceCreateInvalidTTLString(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := `{
		"datacenter": "datacenter1",
		"replicationFactor": 1,
		"ttl": "a",
		"tuuid": false,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `"
	}`

	var respErr = tools.Error{
		Error:   "json: cannot unmarshal string into Go struct field Config.ttl of type int",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), getRandName(), respErr, t)
}

func TestKeyspaceCreateInvalidTTLFloat(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	ttl := "9.1"
	data := `{
		"datacenter": "datacenter1",
		"replicationFactor": 1,
		"ttl": ` + ttl + `,
		"tuuid": false,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `"
	}`

	var respErr = tools.Error{
		Error:   "json: cannot unmarshal number " + ttl + " into Go struct field Config.ttl of type int",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), getRandName(), respErr, t)
}

func TestKeyspaceCreateNilPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	respErr := tools.Error{
		Error:   "EOF",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail(nil, getRandName(), respErr, t)
}

func TestKeyspaceCreateInvalidPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := `{
		"datacenter": "datacenter1",
		"replicationFactor": 1,
		"ttl": 90,
		"tuuid": false,
		"contact": " ` + fmt.Sprintf("test-%v@domain.com", time.Now().Unix()) + `"
	`
	var respErr = tools.Error{
		Error:   "unexpected EOF",
		Message: "Wrong JSON format",
	}

	testKeyspaceCreationFail([]byte(data), getRandName(), respErr, t)
}

// EDIT

func TestKeyspaceEditName(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	dataEdit := tools.KeyspaceEdit{
		Name:    "edit_" + data.Name,
		Contact: data.Contact,
	}
	testKeyspaceEdition(data.ID, dataEdit, t)
}

func TestKeyspaceEditTimeseriesNewContactSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	dataEdit := tools.KeyspaceEdit{
		Name:    data.Name,
		Contact: "test2edit@domain.com",
	}
	testKeyspaceEdition(data.ID, dataEdit, t)
}

func TestKeyspaceEdit2Times(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	dataEdit2 := tools.KeyspaceEdit{
		Name:    "edit_" + getRandName(),
		Contact: "test2edit@domain.com",
	}
	testKeyspaceEdition(data.ID, dataEdit2, t)

	dataEdit3 := tools.KeyspaceEdit{
		Name:    "edit2_" + getRandName(),
		Contact: data.Contact,
	}
	testKeyspaceEdition(data.ID, dataEdit3, t)
}

func TestKeyspaceEditConcurrently(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	dataEdit1 := tools.KeyspaceEdit{
		Name:    "edit1_" + getRandName(),
		Contact: data.Contact,
	}

	dataEdit2 := tools.KeyspaceEdit{
		Name:    "edit2_" + getRandName(),
		Contact: data.Contact,
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	go testKeyspaceEditionConcurrently(data.ID, dataEdit1, dataEdit2, t, &wg)
	go testKeyspaceEditionConcurrently(data.ID, dataEdit2, dataEdit1, t, &wg)

	wg.Wait()
}

func TestKeyspaceEditConflictName(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	data2 := getKeyspace()
	testKeyspaceCreation(&data2, t)

	dataEdit := tools.KeyspaceEdit{
		Name:    data.Name,
		Contact: data2.Contact,
	}

	respErr := tools.Error{
		Error:   "Cannot update because keyspace \"" + data.Name + "\" already exists",
		Message: "Cannot update because keyspace \"" + data.Name + "\" already exists",
	}

	testKeyspaceEditionFail(data2.ID, dataEdit.Marshal(), 409, respErr, t)
}

func TestKeyspaceEditNotExist(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := tools.KeyspaceEdit{
		Name:    fmt.Sprintf("not_exists_%s", getRandName()),
		Contact: "not@exists.com",
	}

	name := "whateverID"

	path := fmt.Sprintf("keyspaces/%s", name)
	code, resp, err := mycenaeTools.HTTP.PUT(path, data.Marshal())
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	assert.Equal(t, 404, code)
	assert.Empty(t, resp)
}

func TestKeyspaceEditEmptyPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	keyspaceBefore := mycenaeTools.Cassandra.Timeseries.CountKeyspacesNoCassWarning()
	keyspaceTsBefore := mycenaeTools.Cassandra.Timeseries.CountTsKeyspacesNoCassWarning()

	path := fmt.Sprintf("keyspaces/%s", data.ID)
	code, _, err := mycenaeTools.HTTP.PUT(path, nil)
	assert.NoError(t, err, "There was an error with request to edit keyspace #%v", data.ID)
	assert.Equal(t, 400, code, "The request to edit keyspace #%v did not return the expected http code", data.ID)

	assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Timeseries.CountKeyspacesNoCassWarning())
	assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Timeseries.CountTsKeyspacesNoCassWarning())
}

func TestKeyspaceEditInvalidNameType(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	data2 := `{"name":2, "contact":` + fmt.Sprintf("test-%d@domain.com", time.Now().Unix()) + `}`

	respErr := tools.Error{
		Error:   "invalid character 'e' in literal true (expecting 'r')",
		Message: "Wrong JSON format",
	}

	testKeyspaceEditionFail(data.ID, []byte(data2), 400, respErr, t)
}

func TestKeyspaceEditInvalidContactOrName(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	cases := map[tools.Error]tools.KeyspaceEdit{
		{Error: errKsContact, Message: errKsContact}: {Name: data.Name, Contact: "test@test@test.com"},
		{Error: errKsContact, Message: errKsContact}: {Name: data.Name, Contact: "test@testcom"},
		{Error: errKsContact, Message: errKsContact}: {Name: data.Name, Contact: "testtest.com"},
		{Error: errKsContact, Message: errKsContact}: {Name: data.Name, Contact: "@test.com"},
		{Error: errKsContact, Message: errKsContact}: {Name: data.Name, Contact: "test@"},
		{Error: errKsContact, Message: errKsContact}: {Name: data.Name, Contact: "test@t est.com"},
		{Error: errKsContact, Message: errKsContact}: {Name: data.Name, Contact: ""},
		{Error: errKsName, Message: errKsName}:       {Name: "test_*123", Contact: data.Contact},
		{Error: errKsName, Message: errKsName}:       {Name: "_test", Contact: data.Contact},
	}

	for err, data2 := range cases {
		testKeyspaceEditionFail(data.ID, data2.Marshal(), 400, err, t)
	}
}

func TestKeyspaceEditMissingField(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	cases := map[tools.Error]string{
		{Error: errKsName, Message: errKsName}:       `{"contact":"` + fmt.Sprintf("test-%d@domain.com", time.Now().Unix()) + `"}`,
		{Error: errKsName, Message: errKsName}:       `{"name":"", "contact":"` + fmt.Sprintf("test-%d@domain.com", time.Now().Unix()) + `"}`,
		{Error: errKsContact, Message: errKsContact}: `{"name":"` + getRandName() + `"}`,
		{Error: errKsContact, Message: errKsContact}: `{"name":"` + getRandName() + `", "contact":""}`,
	}

	for err, dataEdit := range cases {
		testKeyspaceEditionFail(data.ID, []byte(dataEdit), 400, err, t)
	}
}

// LIST

func TestKeyspaceList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreation(&data, t)

	path := fmt.Sprintf("keyspaces")
	code, content, err := mycenaeTools.HTTP.GET(path)
	if err != nil {
		t.Error(err, t)
		t.SkipNow()
	}

	assert.Equal(t, 200, code)
	assert.NotContains(t, string(content), "\"key\":\"macs\"", "The request to list keyspaces should not contains the keyspace macs")
}
