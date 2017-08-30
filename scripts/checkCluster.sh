#!/bin/sh

cs=$(docker inspect --format "{{ .NetworkSettings.IPAddress }}" consulServer)
payload=$(curl -sw "%{http_code}" http://$cs:8500/v1/kv/schema)
status=$(echo -n $payload | tail -c 3)
now=$(date +%s)

if [ "$status" != "404" ]; then
    payload=$(echo $payload | grep -Eo '\"Value\"\:\"[a-zA-Z0-9=]+\"' | sed 's/^.*:"\|"$//g' | /usr/bin/base64 -d)
    time=$(echo $payload | jq -r '.timestamp')
    if [ $(echo "$now-$time" | bc -l) -lt 3600 ]; then
        exit
    fi
fi

schemas=$(docker exec cassandra1 nodetool describecluster | grep -Eo '[a-z0-9-]+: \[([0-9\.]+(,\ )?)*\]' | wc -l)
curl --silent -XPUT http://$cs:8500/v1/kv/schema -d '{"timestamp":'"$now"', "total": '"$schemas"'}'