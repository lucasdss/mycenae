#!/bin/bash

docker rm -f cassandra1 cassandra2 cassandra3 consulCassandra1 consulCassandra2 consulCassandra3 consulServer elastic grafana_mycenae
rm -f /tmp/mycenae/cache.db

mkdir -p /tmp/mycenae

checkCassandraUpNodes () {
    upnodes=$(docker exec -it cassandra1 sh -c "nodetool status" | grep UN | wc -l)
    while [ "$upnodes" != "$1" ]
    do
        sleep 1
        upnodes=$(docker exec -it cassandra1 sh -c "nodetool status" | grep UN | wc -l)
    done
}

./consul_server.sh
./cassandra_with_consul_client.sh 1
./cassandra_with_consul_client.sh 2

checkCassandraUpNodes 2

./cassandra_with_consul_client.sh 3
docker run -d --name elastic -v $GOPATH/src/github.com/uol/mycenae/docs/elasticsearch.yml:/usr/share/elasticsearch/config/elasticsearch.yml elasticsearch:2.4.1
docker run -d --name grafana_mycenae -p 3000:3000 --network=host grafana/grafana:4.2.0

checkCassandraUpNodes 3

docker cp $GOPATH/src/github.com/uol/mycenae/docs/models.cql cassandra1:/tmp/

docker exec -it cassandra1 sh -c "cqlsh -u cassandra -p cassandra < /tmp/models.cql"

docker exec elastic curl --silent -H "Content-Type: application/json" -X POST \
-d '{
	"mappings": {
		"meta": {
			"properties": {
				"tagsNested": {
					"type": "nested",
					"properties": {
						"tagKey": {
							"type": "string"
						},
						"tagValue": {
							"type": "string"
						}
					}
				}
			}
		},
		"metatext": {
			"properties": {
				"tagsNested": {
					"type": "nested",
					"properties": {
						"tagKey": {
							"type": "string"
						},
						"tagValue": {
							"type": "string"
						}
					}
				}
			}
		}
	}
}' http://127.0.0.1:9200/stats

curl --silent -POST -H "Content-Type: application/json" -u admin:admin \
-d '{
	"name": "stats",
	"type": "opentsdb",
	"access": "proxy",
	"url": "http://localhost:8787/keyspaces/stats",
	"basicAuth": false,
	"jsonData": {
		"tsdbResolution": 1,
		"tsdbVersion": 2
	}
}' http://localhost:3000/api/datasources

curl --silent -POST -H "Content-Type: application/json" -u admin:admin -d @../docs/mycenae_dashboard http://localhost:3000/api/dashboards/db

cassandraIPs=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulCassandra1 consulCassandra2 consulCassandra3 | sed 's/^.*$/"&"/' | tr '\n' ',' | sed 's/.$//')
elasticIP=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" elastic)

sed -i 's/nodes = \[[^]]*\]/nodes = \['$cassandraIPs'\]/' ../config.toml
sed -i 's/"[^:]*:9200"/"'$elasticIP':9200"/' ../config.toml

./mycenae_node 0 true