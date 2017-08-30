#!/bin/bash
set -e

name="consulTestMycenae"
pod_name="testMycenae"

docker rm -f ${name} ${pod_name} || true

arguments=(
    '--detach'
    '--hostname' "${name}"
    '--name' "${name}"
    '--dns' '127.0.0.1'
)

server=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulServer)

consul_arguments=(
    'agent'
    '--join' "${server}"
    '--retry-join' "${server}"
    '-recursor' '192.168.206.8'
)

docker run "${arguments[@]}" consul:0.7.2 "${consul_arguments[@]}"

pod_arguments=(
    '--detach'
    '--network' "container:${name}"
    '--name' "${pod_name}"
    '-v' "${GOPATH}/src/github.com/uol/mycenae/tests:/tests/:ro"
    '-v' "${GOPATH}/:/go/:ro"
)

docker run "${pod_arguments[@]}" golang /sbin/init

sleep 3

cassandra1=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulCassandra1)
cassandra2=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulCassandra2)
cassandra3=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulCassandra3)
elastic=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" elastic)
mycenae=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulMycenae0)

docker exec testMycenae /bin/sh -c "echo $cassandra1 cassandra1 >> /etc/hosts"
docker exec testMycenae /bin/sh -c "echo $cassandra2 cassandra2 >> /etc/hosts"
docker exec testMycenae /bin/sh -c "echo $cassandra3 cassandra3 >> /etc/hosts"
docker exec testMycenae /bin/sh -c "echo $elastic elastic >> /etc/hosts"
docker exec testMycenae /bin/sh -c "echo $mycenae mycenae >> /etc/hosts"

docker exec testMycenae go test -timeout 20m -v ../tests/
