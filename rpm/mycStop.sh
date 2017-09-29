#!/bin/bash

project="mycenae"

pkill $project
sleep 5

for ((i=0; i<25; i++))
do

        process=$(/usr/bin/pgrep $project |wc -l)

        if [ $process == 0 ]
        then
                exit
        fi

        sleep 1
done

pkill -9 $project
