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

func getKeyspace() tools.Keyspace {

	data := tools.Keyspace{
		Name:              testUUID(),
		Datacenter:        "datacenter1",
		ReplicationFactor: 1,
		Contact:           fmt.Sprintf("test-%d@domain.com", time.Now().Unix()),
		TTL:               90,
	}

	return data
}

func testKeyspaceFail(data tools.Keyspace, response tools.Error, t *testing.T) {

	body, err := json.Marshal(data)
	if err != nil {
		t.Error(err, response.Message)
		t.SkipNow()
	}

	keyspaceBefore := mycenaeTools.Cassandra.Keyspace.CountKeyspacesNoWarning()
	keyspaceTsBefore := mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesNoWarning()

	path := fmt.Sprintf("keyspaces/%s", data.Name)
	code, resp, err := mycenaeTools.HTTP.POST(path, body)
	if err != nil {
		t.Error(err, response.Message)
		t.SkipNow()
	}

	var respErr tools.Error
	err = json.Unmarshal(resp, &respErr)
	if err != nil {
		t.Error(err, response.Message)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, response.Message)
	assert.Equal(t, response, respErr)
	assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Keyspace.CountKeyspacesNoWarning())
	assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesNoWarning())
}

func testKeyspaceFailStringData(data, name string, response tools.Error, t *testing.T) {

	keyspaceBefore := mycenaeTools.Cassandra.Keyspace.CountKeyspacesNoWarning()
	keyspaceTsBefore := mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesNoWarning()

	path := fmt.Sprintf("keyspaces/%s", name)
	code, resp, err := mycenaeTools.HTTP.POST(path, []byte(data))
	if err != nil {
		t.Error(err, response.Message)
		t.SkipNow()
	}

	var respErr tools.Error
	err = json.Unmarshal(resp, &respErr)
	if err != nil {
		t.Error(err, response.Message)
		t.SkipNow()
	}

	assert.Equal(t, 400, code, response.Message)
	assert.Equal(t, response, respErr)
	assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Keyspace.CountKeyspacesNoWarning())
	assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesNoWarning())
}

func testKeyspaceCreated(data *tools.Keyspace, t *testing.T) {

	body, err := json.Marshal(data)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	path := fmt.Sprintf("keyspaces/%s", data.Name)
	code, content, err := mycenaeTools.HTTP.POST(path, body)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}
	assert.Equal(t, 201, code)

	var ksr tools.KeyspaceResp
	err = json.Unmarshal(content, &ksr)
	if err != nil {
		t.Error(err)
		t.SkipNow()
	}

	data.ID = ksr.KSID
	assert.Contains(t, data.ID, "ts_", "We received a weird keyspace name for request")
	assert.True(t, mycenaeTools.Cassandra.Keyspace.Exists(data.ID), fmt.Sprintf("Keyspace %v was not created", data.ID))
	assert.True(t, mycenaeTools.Cassandra.Keyspace.ExistsInformation(data.ID, data.Name, data.ReplicationFactor, data.Datacenter, data.TTL, false, data.Contact), "Keyspace information was not stored")
}

func testCheckEdition(name, testName string, data interface{}, status int, t *testing.T) {

	body, err := json.Marshal(data)
	if err != nil {
		t.Error(err, testName)
		t.SkipNow()
	}

	//keyspaceBefore := mycenaeTools.Cassandra.Keyspace.CountKeyspacesToken()
	//keyspaceTsBefore := mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesToken()

	path := fmt.Sprintf("keyspaces/%s", name)
	code, _, err := mycenaeTools.HTTP.PUT(path, body)
	if err != nil {
		t.Error(err, testName)
		t.SkipNow()
	}

	assert.Equal(t, status, code, testName)
	//assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Keyspace.CountKeyspacesToken())
	//assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesToken())
}

func checkTables(data tools.Keyspace, t *testing.T) {

	esIndexResponse := mycenaeTools.ElasticSearch.Keyspace.GetIndex(data.ID)
	tableProperties := mycenaeTools.Cassandra.Keyspace.TableProperties(data.ID, "timeseries")

	for _, table := range ttlTables {
		tableProperties = mycenaeTools.Cassandra.Keyspace.TableProperties(data.ID, table)
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
	assert.True(t, mycenaeTools.Cassandra.Keyspace.Exists(data.ID), "Keyspace was not created")
	assert.True(t, mycenaeTools.Cassandra.Keyspace.ExistsInformation(data.ID, data.Name, data.ReplicationFactor, data.Datacenter, data.TTL, false, data.Contact), "Keyspace information was not stored")

	keyspaceCassandraTables := mycenaeTools.Cassandra.Keyspace.KeyspaceTables(data.ID)
	sort.Strings(keyspaceCassandraTables)
	sort.Strings(keyspaceTables)
	assert.Equal(t, keyspaceTables, keyspaceCassandraTables)
	assert.Contains(t, string(esIndexResponse), elasticSearchIndexMeta)
	assert.Contains(t, string(esIndexResponse), elasticSearchIndexMetaText)
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

	cases := map[tools.Error]tools.Keyspace{
		{Error: "Wrong Format: Field \"keyspaceName\" is not well formed. NO information will be saved", Message: "Wrong Format: Field \"keyspaceName\" is not well formed. NO information will be saved"}: {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: "test_*123"},
		{Error: "Wrong Format: Field \"keyspaceName\" is not well formed. NO information will be saved", Message: "Wrong Format: Field \"keyspaceName\" is not well formed. NO information will be saved"}: {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: "_test123"},
		{Error: "Replication factor can not be less than or equal to 0 or greater than 3", Message: "Replication factor can not be less than or equal to 0 or greater than 3"}:                             {Datacenter: dc, ReplicationFactor: 4, TTL: ttl, Contact: contact, Name: testUUID()},
		{Error: "Replication factor can not be less than or equal to 0 or greater than 3", Message: "Replication factor can not be less than or equal to 0 or greater than 3"}:                             {Datacenter: dc, ReplicationFactor: -1, TTL: ttl, Contact: contact, Name: testUUID()},
		{Error: "Replication factor can not be less than or equal to 0 or greater than 3", Message: "Replication factor can not be less than or equal to 0 or greater than 3"}:                             {Datacenter: dc, ReplicationFactor: 0, TTL: ttl, Contact: contact, Name: testUUID()},
		{Error: "Datacenter can not be empty or nil", Message: "Datacenter can not be empty or nil"}:                                                                                                       {Datacenter: "", ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: testUUID()},
		{Error: "Cannot create because datacenter \"dc_error\" not exists", Message: "Cannot create because datacenter \"dc_error\" not exists"}:                                                           {Datacenter: "dc_error", ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: testUUID()},
		{Error: "TTL can not be less or equal to zero", Message: "TTL can not be less or equal to zero"}:                                                                                                   {Datacenter: dc, ReplicationFactor: rf, TTL: 91, Contact: contact, Name: testUUID()},
		{Error: "TTL can not be less or equal to zero", Message: "TTL can not be less or equal to zero"}:                                                                                                   {Datacenter: dc, ReplicationFactor: rf, TTL: -10, Contact: contact, Name: testUUID()},
		{Error: "TTL can not be less or equal to zero", Message: "TTL can not be less or equal to zero"}:                                                                                                   {Datacenter: dc, ReplicationFactor: rf, TTL: 0, Contact: contact, Name: testUUID()},
		{Error: "Contact field should be a valid email address", Message: "Contact field should be a valid email address"}:                                                                                 {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "test@test@test.com", Name: testUUID()},
		{Error: "Contact field should be a valid email address", Message: "Contact field should be a valid email address"}:                                                                                 {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "test@testcom", Name: testUUID()},
		{Error: "Contact field should be a valid email address", Message: "Contact field should be a valid email address"}:                                                                                 {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "testtest.com", Name: testUUID()},
		{Error: "Contact field should be a valid email address", Message: "Contact field should be a valid email address"}:                                                                                 {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "@test.com", Name: testUUID()},
		{Error: "Contact field should be a valid email address", Message: "Contact field should be a valid email address"}:                                                                                 {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "test@", Name: testUUID()},
		{Error: "Contact field should be a valid email address", Message: "Contact field should be a valid email address"}:                                                                                 {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Contact: "test@t est.com", Name: testUUID()},
	}

	for err, ks := range cases {
		testKeyspaceFail(ks, err, t)
	}
}

func TestKeyspaceCreateNewTimeseriesErrorMissingField(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var (
		dc      = "datacenter1"
		rf      = 1
		ttl     = 90
		contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
		name    = testUUID()
	)

	cases := map[tools.Error]tools.Keyspace{
		{Error: "Datacenter can not be empty or nil", Message: "Datacenter can not be empty or nil"}:                                                                           {ReplicationFactor: rf, TTL: ttl, Contact: contact, Name: name},
		{Error: "Replication factor can not be less than or equal to 0 or greater than 3", Message: "Replication factor can not be less than or equal to 0 or greater than 3"}: {Datacenter: dc, TTL: ttl, Contact: contact, Name: name},
		{Error: "TTL can not be less or equal to zero", Message: "TTL can not be less or equal to zero"}:                                                                       {Datacenter: dc, ReplicationFactor: rf, Contact: contact, Name: name},
		{Error: "Contact field should be a valid email address", Message: "Contact field should be a valid email address"}:                                                     {Datacenter: dc, ReplicationFactor: rf, TTL: ttl, Name: name},
	}

	for err, ks := range cases {
		testKeyspaceFail(ks, err, t)
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
		assert.Equal(t, 409, code)
	}
}

func TestKeyspaceCreateNewTimeseriesNewErrorInvalidRFString(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	// type keyspace struct {
	// 	DC      *string `json:"datacenter,omitempty"`
	// 	Factor  *string `json:"replicationFactor,omitempty"`
	// 	TTL     *int    `json:"ttl,omitempty"`
	// 	TUUID   *bool   `json:"tuuid,omitempty"`
	// 	Contact *string
	// 	// --- Internal Data ---
	// 	name string
	// }

	// var (
	// 	dc      = "datacenter1"
	// 	rf      = "a"
	// 	ttl     = 90
	// 	tuuid   = false
	// 	contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	// )

	// data := keyspace{
	// 	&dc, &rf, &ttl, &tuuid, &contact, testUUID(),
	// }

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

	testKeyspaceFailStringData(data, testUUID(), respErr, t)
}

func TestKeyspaceCreateNewTimeseriesNewErrorInvalidRFFloat(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	// type keyspace struct {
	// 	DC      *string  `json:"datacenter,omitempty"`
	// 	Factor  *float64 `json:"replicationFactor,omitempty"`
	// 	TTL     *int     `json:"ttl,omitempty"`
	// 	TUUID   *bool    `json:"tuuid,omitempty"`
	// 	Contact *string
	// 	// --- Internal Data ---
	// 	name string
	// }

	// var (
	// 	dc      = "datacenter1"
	// 	rf      = 1.1
	// 	ttl     = 90
	// 	tuuid   = false
	// 	contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	// )

	// data := keyspace{
	// 	&dc, &rf, &ttl, &tuuid, &contact, testUUID(),
	// }

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

	testKeyspaceFailStringData(data, testUUID(), respErr, t)
}

func TestKeyspaceCreateNewErrorInvalidTTLString(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	// type keyspace struct {
	// 	DC      *string `json:"datacenter,omitempty"`
	// 	Factor  *int    `json:"replicationFactor,omitempty"`
	// 	TTL     *string `json:"ttl,omitempty"`
	// 	TUUID   *bool   `json:"tuuid,omitempty"`
	// 	Contact *string
	// 	// --- Internal Data ---
	// 	name string
	// }

	// var (
	// 	dc      = "datacenter1"
	// 	rf      = 1
	// 	ttl     = "a"
	// 	tuuid   = false
	// 	contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	// )

	// data := keyspace{
	// 	&dc, &rf, &ttl, &tuuid, &contact, testUUID(),
	// }

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

	testKeyspaceFailStringData(data, testUUID(), respErr, t)
}

func TestKeyspaceCreateNewErrorInvalidTTLFloat(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	// type keyspace struct {
	// 	DC      *string  `json:"datacenter,omitempty"`
	// 	Factor  *int     `json:"replicationFactor,omitempty"`
	// 	TTL     *float64 `json:"ttl,omitempty"`
	// 	TUUID   *bool    `json:"tuuid,omitempty"`
	// 	Contact *string
	// 	// --- Internal Data ---
	// 	name string
	// }

	// var (
	// 	dc      = "datacenter1"
	// 	rf      = 1
	// 	ttl     = 9.1
	// 	tuuid   = false
	// 	contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	// )

	// data := keyspace{
	// 	&dc, &rf, &ttl, &tuuid, &contact, testUUID(),
	// }

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

	testKeyspaceFailStringData(data, testUUID(), respErr, t)
}

func TestKeyspaceCreateNewTimeseriesNewErrorNilPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	keyspaceBefore := mycenaeTools.Cassandra.Keyspace.CountKeyspacesNoWarning()
	keyspaceTsBefore := mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesNoWarning()

	path := fmt.Sprintf("keyspaces/%s", testUUID())
	code, _, err := mycenaeTools.HTTP.POST(path, nil)
	assert.NoError(t, err, "There was an error with request")
	assert.Equal(t, 400, code)

	assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Keyspace.CountKeyspacesNoWarning())
	assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesNoWarning())
}

func TestKeyspaceCreateNewTimeseriesNewErrorEmptyPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	var payload []byte
	keyspaceBefore := mycenaeTools.Cassandra.Keyspace.CountKeyspacesNoWarning()
	keyspaceTsBefore := mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesNoWarning()

	path := fmt.Sprintf("keyspaces/%s", testUUID())
	code, _, err := mycenaeTools.HTTP.POST(path, payload)
	assert.NoError(t, err, "There was an error with request")
	assert.Equal(t, 400, code)

	assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Keyspace.CountKeyspacesNoWarning())
	assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesNoWarning())
}

func TestKeyspaceCreateNewTimeseriesNewErrorInvalidPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	// type keyspace struct {
	// 	DC      *string `json:"invalidDatacenter,omitempty"`
	// 	Factor  *int    `json:"invalidReplicationFactor,omitempty"`
	// 	TTL     *int    `json:"invalidTTL,omitempty"`
	// 	TUUID   *bool   `json:"tuuid,omitempty"`
	// 	Contact *string
	// 	// --- Internal Data ---
	// 	name string
	// }

	// var (
	// 	dc      = "datacenter1"
	// 	rf      = 1
	// 	ttl     = 90
	// 	tuuid   = false
	// 	contact = fmt.Sprintf("test-%v@domain.com", time.Now().Unix())
	// )

	// data := keyspace{
	// 	&dc, &rf, &ttl, &tuuid, &contact, testUUID(),
	// }

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

	testKeyspaceFailStringData(data, testUUID(), respErr, t)
}

// EDIT

func TestKeyspaceEditTimeseriesNewNameSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	data2 := tools.KeyspaceEdit{
		Name:    "edit_" + data.Name,
		Contact: data.Contact,
	}
	testCheckEdition(data.ID, "", data2, 200, t)
}

func TestKeyspaceEditTimeseriesNewContactSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	data2 := tools.KeyspaceEdit{
		Name:    data.Name,
		Contact: "test2edit@domain.com",
	}
	testCheckEdition(data.ID, "", data2, 200, t)
}

func TestKeyspaceEdit2TimesTimeseriesNewSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	data2 := tools.KeyspaceEdit{
		Name:    "edit_" + testUUID(),
		Contact: "test2edit@domain.com",
	}
	testCheckEdition(data.ID, "", data2, 200, t)

	data3 := tools.KeyspaceEdit{
		Name:    "edit2_" + testUUID(),
		Contact: data.Contact,
	}
	testCheckEdition(data.ID, "", data3, 200, t)
}

func TestKeyspaceEditTimeseriesNewSuccessConcurrent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	path := fmt.Sprintf("keyspaces/%s", data.ID)
	body, _ := json.Marshal(tools.KeyspaceEdit{
		Name:    "edit1_" + testUUID(),
		Contact: data.Contact,
	})
	body2, _ := json.Marshal(tools.KeyspaceEdit{
		Name:    "edit2_" + testUUID(),
		Contact: data.Contact,
	})
	count := 0

	keyspaceBefore := mycenaeTools.Cassandra.Keyspace.CountKeyspacesNoWarning()
	keyspaceTsBefore := mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesNoWarning()

	go func() {
		code, _, err := mycenaeTools.HTTP.PUT(path, body)
		assert.NoError(t, err, "There was an error with request to edit keyspace #%v", data.ID)
		assert.Equal(t, 200, code, "The request to edit keyspace #%v did not return the expected http code", data.ID)
		count++
	}()
	go func() {
		code, _, err := mycenaeTools.HTTP.PUT(path, body2)
		assert.NoError(t, err, "There was an error with request to edit keyspace #%v", data.ID)
		assert.Equal(t, 200, code, "The request to edit keyspace #%v did not return the expected http code", data.ID)
		count++
	}()

	for count < 2 {
		time.Sleep(1 * time.Second)
	}
	assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Keyspace.CountKeyspacesNoWarning())
	assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesNoWarning())
}

func TestKeyspaceEditTimeseriesNewErrorConflictName(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	data2 := getKeyspace()
	testKeyspaceCreated(&data2, t)

	data3 := tools.KeyspaceEdit{
		Name:    data2.Name,
		Contact: data2.Contact,
	}
	testCheckEdition(data.ID, "ConflictName", data3, 409, t)
}

func TestKeyspaceEditTimeseriesNewErrorDoesNotExists(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	data := tools.KeyspaceEdit{
		Name:    fmt.Sprintf("not_exists_%s", testUUID()),
		Contact: "not@exists.com",
	}
	testCheckEdition(data.Name, "EditNameDoesNotExist", data, 404, t)

}

func TestKeyspaceEditTimeseriesNewErrorEmptyPayload(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	data := getKeyspace()
	testKeyspaceCreated(&data, t)

	keyspaceBefore := mycenaeTools.Cassandra.Keyspace.CountKeyspacesNoWarning()
	keyspaceTsBefore := mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesNoWarning()

	path := fmt.Sprintf("keyspaces/%s", data.ID)
	code, _, err := mycenaeTools.HTTP.PUT(path, nil)
	assert.NoError(t, err, "There was an error with request to edit keyspace #%v", data.ID)
	assert.Equal(t, 400, code, "The request to edit keyspace #%v did not return the expected http code", data.ID)

	assert.Equal(t, keyspaceBefore, mycenaeTools.Cassandra.Keyspace.CountKeyspacesNoWarning())
	assert.Equal(t, keyspaceTsBefore, mycenaeTools.Cassandra.Keyspace.CountTsKeyspacesNoWarning())
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

	cases := map[string]tools.KeyspaceEdit{
		"EditInvalidContact1":    {Name: data.Name, Contact: "test@test@test.com"},
		"EditInvalidContact2":    {Name: data.Name, Contact: "test@testcom"},
		"EditInvalidContact3":    {Name: data.Name, Contact: "testtest.com"},
		"EditInvalidContact4":    {Name: data.Name, Contact: "@test.com"},
		"EditInvalidContact5":    {Name: data.Name, Contact: "test@"},
		"EditInvalidContact6":    {Name: data.Name, Contact: "test@t est.com"},
		"EditEmptyContact":       {Name: data.Name, Contact: ""},
		"EditBadName":            {Name: "test_*123", Contact: data.Contact},
		"EditBadNameStartsWith_": {Name: "_test", Contact: data.Contact},
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

	data2 := tools.KeyspaceEdit{
		Name:    "",
		Contact: data.Contact,
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
