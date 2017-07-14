#!/bin/bash

name="consulCassandra$1"
pod_name="cassandra$1"
path="$GOPATH/src/github.com/uol/mycenae"

arguments=(
    '--detach'
    '--hostname' "${name}"
    '--name' "${name}"
    '--dns' "127.0.0.1"
)

server=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulServer)

consul_arguments=(
    'agent'
    '--join' "${server}"
    '--retry-join' "${server}"
    '-recursor' "192.168.206.8"
    '-disable-host-node-id'
)

sleep 10

docker run "${arguments[@]}" consul:0.8.3 "${consul_arguments[@]}"

pod_arguments=(
    '--detach'
    '--network' "container:${name}"
    '--name' "${pod_name}"
    '-v' "${GOPATH}/:/go/:ro"
    '-e' 'CASSANDRA_CLUSTER_NAME=CassandraCluster'
    '-v' "$path/scripts/changeAuthentication.sh:/tmp/changeAuthentication.sh"
    '--entrypoint' '/tmp/changeAuthentication.sh'
)

if [ $1 -gt 1 ]
    then
        seedIP="$(docker inspect --format='{{ .NetworkSettings.IPAddress }}' consulCassandra1)"
        pod_arguments=( ${pod_arguments[@]} '-e' "CASSANDRA_SEEDS=$seedIP" )
fi

docker run "${pod_arguments[@]}" "cassandra:3.0.9" /sbin/init

client=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" "$name")

docker exec $name curl --silent -XPUT -d '{"name":"cassandra","port":9042}' --header "Content-type: application/json" "http://localhost:8500/v1/agent/service/register"