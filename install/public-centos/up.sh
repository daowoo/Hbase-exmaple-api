#!/bin/bash

for i in `seq -f '%g' 1 $1`;
do
  vagrant up hbase$i
done
