package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/mycenae/tests/tools"
)

type ks struct {
	Name    string
	Contact string
}

var keyspaceTables = []string{"timeseries", "ts_text_stamp"}
var ttlTables = []string{"timeseries", "ts_text_stamp"}
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

type keyspaceResponse struct {
	Ksid string
}

func getKeyspace() (tools.Keyspace) {

	data := tools.Keyspace{
		Name: testUUID(),
		Datacenter: "datacenter1",
		ReplicationFactor: 1,
		Contact: fmt.Sprintf("test-%d@domain.com", time.Now().Unix()),
		TTL: 90,
	}

	return data
}

func testKeyspaceFail(data interface{}, name, test string, t *testing.T) {

	body, err := json.Marshal(data)
	assert.NoError(t, err, "Could not marshal data", test)

	keyspaceBefore := mycenaeTools.Cassandra.Keyspace.CountKeyspaces()
	keyspaceTsBefore := mycenaeTools.Cassandra.Keyspace.CountTsKeyspaces()

	path := fmt.Sprintf("keyspaces/%s", name)
	code, _, err := mycenaeTools.HTTP.POST(path, body)
	//code, resp, err := mycenaeTools.HTTP.POST(path, body)
	assert.NoError(t, err, "There was an error with request", test)

	//fmt.Printf("\n%s: %vdata: %v\n", test, string(resp), string(body))

	assert.Equal(t, 400, code, "The request did not return the expected http code", test)
	assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Keyspace.CountKeyspaces())
	assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Keyspace.CountTsKeyspaces())
}

func testKeyspaceCreated(data *tools.Keyspace, t *testing.T) {

	body, err := json.Marshal(data)
	assert.NoError(t, err, "Could not marshal value")

	path := fmt.Sprintf("keyspaces/%s", data.Name)
	code, content, err := mycenaeTools.HTTP.POST(path, body)

	assert.NoError(t, err, "There was an error with request")
	assert.Equal(t, 201, code, "The request did not return the expected http code")

	var ksr keyspaceResponse
	err = json.Unmarshal(content, &ksr)
	assert.NoError(t, err, "It was not possible to unmarshal the response for request")

	data.Keyspace = ksr.Ksid
	assert.Contains(t, data.Keyspace, "ts_", "We received a weird keyspace name for request")
	assert.True(t, mycenaeTools.Cassandra.Keyspace.Exists(data.Keyspace), fmt.Sprintf("Keyspace %v was not created", data.Keyspace))
	assert.True(t, mycenaeTools.Cassandra.Keyspace.ExistsInformation(data.Keyspace, data.Name, data.ReplicationFactor, data.Datacenter, data.TTL, false, data.Contact), "Keyspace information was not stored")
}

func testCheckEdition(name, testName string, data2 interface{}, status int, t *testing.T) {

	body, err := json.Marshal(data2)
	assert.NoError(t, err, "Could not marshal value", testName)

	//keyspaceBefore := mycenaeTools.Cassandra.Keyspace.CountKeyspaces()
	//keyspaceTsBefore := mycenaeTools.Cassandra.Keyspace.CountTsKeyspaces()

	path := fmt.Sprintf("keyspaces/%s", name)
	code, _, err := mycenaeTools.HTTP.PUT(path, body)
	//code, resp, err := mycenaeTools.HTTP.PUT(path, body)
	assert.NoError(t, err, "There was an error with request", testName)

	//fmt.Printf("\n%s: %vdata: %v\n", testName, string(resp), string(body))

	assert.Equal(t, status, code, "The request did not return the expected http code", testName)
	//assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Keyspace.CountKeyspaces())
	//assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Keyspace.CountTsKeyspaces())
}

func checkTables(data tools.Keyspace, t *testing.T) {

	esIndexResponse := mycenaeTools.ElasticSearch.Keyspace.GetIndex(data.Keyspace)
	tableProperties := mycenaeTools.Cassandra.Keyspace.TableProperties(data.Keyspace, "timeseries")

	for _, table := range ttlTables {
		tableProperties = mycenaeTools.Cassandra.Keyspace.TableProperties(data.Keyspace, table)
		assert.Exactly(t, 0.01, tableProperties.Bloom_filter_fp_chance, "Properties don't match")
		assert.Exactly(t, caching, tableProperties.Caching, "Properties don't match")
		assert.Exactly(t, "", tableProperties.Comment, "Properties don't match")
		assert.Exactly(t, compaction, tableProperties.Compaction, "Properties don't match")
		assert.Exactly(t, compression, tableProperties.Compression, "Properties don't match")
		assert.Exactly(t, 0.0, tableProperties.Dclocal_read_repair_chance, "Properties don't match")
		assert.Exactly(t, data.TTL*86400, tableProperties.Default_time_to_live, "Properties don't match")
		assert.Exactly(t, 0, tableProperties.Gc_grace_seconds, "Properties don't match")
		assert.Exactly(t, 2048, tableProperties.Max_index_interval, "Properties don't match")
		assert.Exactly(t, 0, tableProperties.Memtable_flush_period_in_ms, "Properties don't match")
		assert.Exactly(t, 128, tableProperties.Min_index_interval, "Properties don't match")
		assert.Exactly(t, 0.0, tableProperties.Read_repair_chance, "Properties don't match")
		assert.Exactly(t, "99PERCENTILE", tableProperties.Speculative_retry, "Properties don't match")
	}

	assert.Contains(t, data.Keyspace, "ts_", "We received a weird keyspace name for request")
	assert.True(t, mycenaeTools.Cassandra.Keyspace.Exists(data.Keyspace), "Keyspace was not created")
	assert.True(t, mycenaeTools.Cassandra.Keyspace.ExistsInformation(data.Keyspace, data.Name, data.ReplicationFactor, data.Datacenter, data.TTL, false, data.Contact), "Keyspace information was not stored")

	keyspaceCassandraTables := mycenaeTools.Cassandra.Keyspace.KeyspaceTables(data.Keyspace)
	sort.Strings(keyspaceCassandraTables)
	sort.Strings(keyspaceTables)
	assert.Equal(t, keyspaceTables, keyspaceCassandraTables, fmt.Sprintf("FOUND: %v, EXPECTED: %v", keyspaceCassandraTables, keyspaceCassandraTables))
	assert.Contains(t, string(esIndexResponse), elasticSearchIndexMeta, "Elastic Search index don't match %v, %v")
	assert.Contains(t, string(esIndexResponse), elasticSearchIndexMetaText, "Elastic Search index don't match %v, %v")
}

func testUUID() string {
	return fmt.Sprintf("test_%d", time.Now().UnixNano())
}

// CREATE

func TestKeyspaceCreateNewTimeseriesError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var (
		dc      = "datacenter1"
		rf      = 1
		ttl     = 90
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	cases := map[string]struct {
		DC       string `json:"datacenter"`
		RF       int    `json:"replicationFactor"`
		TTL      int    `json:"ttl"`
		TUUID    bool   `json:"tuuid"`
		Contact  string `json:"contact"`
		Name     string `json:"name"`
		Keyspace string `json:"keyspace"`
	}{
		"BadName":            {dc, rf, ttl, false, contact, "test_*123", ""},
		"BadNameStartsWith_": {dc, rf, ttl, false, contact, "_test123", ""},
		"RF4":                {dc, 4, ttl, false, contact, testUUID(), ""},
		"NegativeRF":         {dc, -1, ttl, false, contact, testUUID(), ""},
		"RF0":                {dc, 0, ttl, false, contact, testUUID(), ""},
		"EmptyDC":            {"", rf, ttl, false, contact, testUUID(), ""},
		"DCDoesNotExist":     {"dc_error", rf, ttl, false, contact, testUUID(), ""},
		"TTLAboveMax":        {dc, rf, 91, false, contact, testUUID(), ""},
		"NegativeTTL":        {dc, rf, -10, false, contact, testUUID(), ""},
		"TTL0":               {dc, rf, 0, false, contact, testUUID(), ""},
		"InvalidContact1":    {dc, rf, ttl, false, "test@test@test.com", testUUID(), ""},
		"InvalidContact2":    {dc, rf, ttl, false, "test@testcom", testUUID(), ""},
		"InvalidContact3":    {dc, rf, ttl, false, "testtest.com", testUUID(), ""},
		"InvalidContact4":    {dc, rf, ttl, false, "@test.com", testUUID(), ""},
		"InvalidContact5":    {dc, rf, ttl, false, "test@", testUUID(), ""},
		"InvalidContact6":    {dc, rf, ttl, false, "test@t est.com", testUUID(), ""},
	}

	for test, ks := range cases {
		testKeyspaceFail(ks, ks.Name, test, t)
	}
}

func TestKeyspaceCreateNewTimeseriesErrorMissingField(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var (
		dc       = "datacenter1"
		rf       = 1
		ttl      = 90
		tuuid    = false
		contact  = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
		name     = testUUID()
		keyspace = ""
	)

	cases := map[string]struct {
		DC       *string `json:"datacenter,omitempty"`
		RF       *int    `json:"replicationFactor,omitempty"`
		TTL      *int    `json:"ttl,omitempty"`
		TUUID    *bool   `json:"tuuid,omitempty"`
		Contact  *string `json:"contact,omitempty"`
		Name     *string `json:"name,omitempty"`
		Keyspace *string `json:"keyspace,omitempty"`
	}{
		"DCNil":      {nil, &rf, &ttl, &tuuid, &contact, &name, &keyspace},
		"RFNil":      {&dc, nil, &ttl, &tuuid, &contact, &name, &keyspace},
		"TTLNil":     {&dc, &rf, nil, &tuuid, &contact, &name, &keyspace},
		"ContactNil": {&dc, &rf, &ttl, &tuuid, nil, &name, &keyspace},
	}

	for test, ks := range cases {
		testKeyspaceFail(ks, *ks.Name, test, t)
	}
}

func TestKeyspaceCreateNewTimeseriesNewSuccessRF1(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	data.ReplicationFactor = 1
	testKeyspaceCreated(&data, t)
	checkTables(data, t)
}

func TestKeyspaceCreateNewTimeseriesNewSuccessRF2(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	data.ReplicationFactor = 2
	testKeyspaceCreated(&data, t)
	checkTables(data, t)
}

func TestKeyspaceCreateNewTimeseriesNewSuccessRF3(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	data.ReplicationFactor = 3
	testKeyspaceCreated(&data, t)
	checkTables(data, t)
}

func TestKeyspaceCreateNewTimeseriesNewErrorConflict(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	body, err := json.Marshal(data)
	assert.NoError(t, err, "Could not marshal data")

	// Attempt to create again to validate Conflict
	path := fmt.Sprintf("keyspaces/%s", data.Name)
	code, _, err := mycenaeTools.HTTP.POST(path, body)
	if assert.NoError(t, err, "There was an error with request") {
		assert.Equal(t, 409, code, "The request did not return the expected http code")
	}
}

func TestKeyspaceCreateNewTimeseriesNewErrorInvalidRFString(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	type keyspace struct {
		DC      *string `json:"datacenter,omitempty"`
		Factor  *string `json:"replicationFactor,omitempty"`
		TTL     *int    `json:"ttl,omitempty"`
		TUUID   *bool   `json:"tuuid,omitempty"`
		Contact *string
		// --- Internal Data ---
		name string
	}

	var (
		dc      = "datacenter1"
		rf      = "a"
		ttl     = 90
		tuuid   = false
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	data := keyspace{
		&dc, &rf, &ttl, &tuuid, &contact, testUUID(),
	}
	testKeyspaceFail(data, data.name, "", t)
}

func TestKeyspaceCreateNewTimeseriesNewErrorInvalidRFFloat(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	type keyspace struct {
		DC      *string  `json:"datacenter,omitempty"`
		Factor  *float64 `json:"replicationFactor,omitempty"`
		TTL     *int     `json:"ttl,omitempty"`
		TUUID   *bool    `json:"tuuid,omitempty"`
		Contact *string
		// --- Internal Data ---
		name string
	}

	var (
		dc      = "datacenter1"
		rf      = 1.1
		ttl     = 90
		tuuid   = false
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	data := keyspace{
		&dc, &rf, &ttl, &tuuid, &contact, testUUID(),
	}
	testKeyspaceFail(data, data.name, "", t)
}

func TestKeyspaceCreateNewErrorInvalidTTLString(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	type keyspace struct {
		DC      *string `json:"datacenter,omitempty"`
		Factor  *int    `json:"replicationFactor,omitempty"`
		TTL     *string `json:"ttl,omitempty"`
		TUUID   *bool   `json:"tuuid,omitempty"`
		Contact *string
		// --- Internal Data ---
		name string
	}

	var (
		dc      = "datacenter1"
		rf      = 1
		ttl     = "a"
		tuuid   = false
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	data := keyspace{
		&dc, &rf, &ttl, &tuuid, &contact, testUUID(),
	}
	testKeyspaceFail(data, data.name, "", t)
}

func TestKeyspaceCreateNewErrorInvalidTTLFloat(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	type keyspace struct {
		DC      *string  `json:"datacenter,omitempty"`
		Factor  *int     `json:"replicationFactor,omitempty"`
		TTL     *float64 `json:"ttl,omitempty"`
		TUUID   *bool    `json:"tuuid,omitempty"`
		Contact *string
		// --- Internal Data ---
		name string
	}

	var (
		dc      = "datacenter1"
		rf      = 1
		ttl     = 9.1
		tuuid   = false
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	data := keyspace{
		&dc, &rf, &ttl, &tuuid, &contact, testUUID(),
	}
	testKeyspaceFail(data, data.name, "", t)
}

func TestKeyspaceCreateNewTimeseriesNewErrorNilPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	keyspaceBefore := mycenaeTools.Cassandra.Keyspace.CountKeyspaces()
	keyspaceTsBefore := mycenaeTools.Cassandra.Keyspace.CountTsKeyspaces()

	path := fmt.Sprintf("keyspaces/%s", testUUID())
	code, _, err := mycenaeTools.HTTP.POST(path, nil)
	assert.NoError(t, err, "There was an error with request")
	assert.Equal(t, 400, code, "The request did not return the expected http code")

	assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Keyspace.CountKeyspaces())
	assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Keyspace.CountTsKeyspaces())
}

func TestKeyspaceCreateNewTimeseriesNewErrorEmptyPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var payload []byte
	keyspaceBefore := mycenaeTools.Cassandra.Keyspace.CountKeyspaces()
	keyspaceTsBefore := mycenaeTools.Cassandra.Keyspace.CountTsKeyspaces()

	path := fmt.Sprintf("keyspaces/%s", testUUID())
	code, _, err := mycenaeTools.HTTP.POST(path, payload)
	assert.NoError(t, err, "There was an error with request")
	assert.Equal(t, 400, code, "The request did not return the expected http code")

	assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Keyspace.CountKeyspaces())
	assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Keyspace.CountTsKeyspaces())
}

func TestKeyspaceCreateNewTimeseriesNewErrorInvalidPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	type keyspace struct {
		DC      *string `json:"invalidDatacenter,omitempty"`
		Factor  *int    `json:"invalidReplicationFactor,omitempty"`
		TTL     *int    `json:"invalidTTL,omitempty"`
		TUUID   *bool   `json:"tuuid,omitempty"`
		Contact *string
		// --- Internal Data ---
		name string
	}

	var (
		dc      = "datacenter1"
		rf      = 1
		ttl     = 90
		tuuid   = false
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	)

	data := keyspace{
		&dc, &rf, &ttl, &tuuid, &contact, testUUID(),
	}
	testKeyspaceFail(data, data.name, "", t)
}

// EDIT

func TestKeyspaceEditTimeseriesNewNameSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	data2 := ks{
		"edit_" + data.Name,
		data.Contact,
	}
	testCheckEdition(data.Keyspace, "", data2, 200, t)
}

func TestKeyspaceEditTimeseriesNewContactSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	data2 := ks{
		data.Name,
		"test2edit@domain.com",
	}
	testCheckEdition(data.Keyspace, "", data2, 200, t)
}

func TestKeyspaceEdit2TimesTimeseriesNewSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	data2 := ks{
		"edit_" + testUUID(),
		"test2edit@domain.com",
	}
	testCheckEdition(data.Keyspace, "", data2, 200, t)

	data3 := ks{
		"edit2_" + testUUID(),
		data.Contact,
	}
	testCheckEdition(data.Keyspace, "", data3, 200, t)
}

func TestKeyspaceEditTimeseriesNewSuccessConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	path := fmt.Sprintf("keyspaces/%s", data.Keyspace)
	body, _ := json.Marshal(ks{
		"edit1_" + testUUID(),
		data.Contact,
	})
	body2, _ := json.Marshal(ks{
		"edit2_" + testUUID(),
		data.Contact,
	})
	count := 0

	keyspaceBefore := mycenaeTools.Cassandra.Keyspace.CountKeyspaces()
	keyspaceTsBefore := mycenaeTools.Cassandra.Keyspace.CountTsKeyspaces()

	go func() {
		code, _, err := mycenaeTools.HTTP.PUT(path, body)
		assert.NoError(t, err, "There was an error with request to edit keyspace #%v", data.Keyspace)
		assert.Equal(t, 200, code, "The request to edit keyspace #%v did not return the expected http code", data.Keyspace)
		count++
	}()
	go func() {
		code, _, err := mycenaeTools.HTTP.PUT(path, body2)
		assert.NoError(t, err, "There was an error with request to edit keyspace #%v", data.Keyspace)
		assert.Equal(t, 200, code, "The request to edit keyspace #%v did not return the expected http code", data.Keyspace)
		count++
	}()

	for count < 2 {
		time.Sleep(1 * time.Second)
	}
	assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Keyspace.CountKeyspaces())
	assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Keyspace.CountTsKeyspaces())
}

func TestKeyspaceEditTimeseriesNewErrorConflictName(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	data2 := getKeyspace()
	testKeyspaceCreated(&data2, t)

	data3 := ks{
		Name: data2.Name,
		Contact: data2.Contact,
	}
	testCheckEdition(data.Keyspace, "ConflictName", data3, 409, t)
}

func TestKeyspaceEditTimeseriesNewErrorDoesNotExists(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	data := ks{
		fmt.Sprintf("not_exists_%s", testUUID()),
		"not@exists.com",
	}
	testCheckEdition(data.Name, "EditNameDoesNotExist", data, 404, t)

}

func TestKeyspaceEditTimeseriesNewErrorEmptyPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	keyspaceBefore := mycenaeTools.Cassandra.Keyspace.CountKeyspaces()
	keyspaceTsBefore := mycenaeTools.Cassandra.Keyspace.CountTsKeyspaces()

	path := fmt.Sprintf("keyspaces/%s", data.Keyspace)
	code, _, err := mycenaeTools.HTTP.PUT(path, nil)
	assert.NoError(t, err, "There was an error with request to edit keyspace #%v", data.Keyspace)
	assert.Equal(t, 400, code, "The request to edit keyspace #%v did not return the expected http code", data.Keyspace)

	assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Keyspace.CountKeyspaces())
	assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Keyspace.CountTsKeyspaces())
}

func TestKeyspaceEditTimeseriesNewErrorInvalidPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	type ks struct {
		Name    int
		Contact string
	}

	data2 := ks{
		1,
		data.Contact,
	}
	testCheckEdition(data.Name, "InvalidPayload", data2, 400, t)
}

func TestKeyspaceEditNewTimeseriesNewErrorInvalidContactAndName(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	cases := map[string]ks{
		"EditInvalidContact1": {data.Name, "test@test@test.com"},
		"EditInvalidContact2": {data.Name, "test@testcom"},
		"EditInvalidContact3": {data.Name, "testtest.com"},
		"EditInvalidContact4": {data.Name, "@test.com"},
		"EditInvalidContact5": {data.Name, "test@"},
		"EditInvalidContact6": {data.Name, "test@t est.com"},
		"EditEmptyContact":    {data.Name, ""},

		"EditBadName":            {"test_*123", data.Contact},
		"EditBadNameStartsWith_": {"_test", data.Contact},
	}

	for testName, data2 := range cases {
		testCheckEdition(data.Name, testName, data2, 400, t)
	}
}

func TestKeyspaceEditNewTimeseriesNewErrorEmptyName(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	data2 := ks{
		"",
		data.Contact,
	}
	testCheckEdition(data.Name, "EmptyName", data2, 400, t)
}

func TestKeyspaceEditNewTimeseriesNewErrorMissingField(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	cases := map[string]struct {
		Name    *string
		Contact *string
	}{
		"NameNil":    {nil, &data.Contact},
		"ContactNil": {&data.Name, nil},
	}

	for test, data2 := range cases {
		testCheckEdition(data.Name, test, data2, 400, t)
	}
}

// LIST

func TestKeyspaceListTimeseriesNew(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	path := fmt.Sprintf("keyspaces")
	code, content, err := mycenaeTools.HTTP.GET(path)
	assert.NoError(t, err, "There was an error with request to list keyspaces")
	assert.Equal(t, 200, code, "The request to list keyspaces did not return the expected http code")
	assert.NotContains(t, string(content), "\"key\":\"macs\"", "The request to list keyspaces should not contains the keyspace macs")
}
