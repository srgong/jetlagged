#!/usr/bin/env bash

IP_ADDR=$1
NUM_SPAWNS=$2
SESSION=$3
scalac FareStreamer.scala
tmux new-session -s $SESSION -n bash -d
for ID in `seq 1 $NUM_SPAWNS`;
do
    echo $ID
    tmux new-window -t $ID
    tmux send-keys -t $SESSION:$ID 'scala FareStreamer '"$IP_ADDR"' '"$ID"'' C-m
done