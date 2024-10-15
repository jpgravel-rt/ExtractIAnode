#!/bin/bash
for i in $(seq 1 20); do
    echo "##################### Loop $i of 20 ######################" >> logs-stdout.txt
    /opt/R/3.6.1/bin/Rscript main-backward.R >> logs-stdout.txt 2>> logs-stderr.txt
done
