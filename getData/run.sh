#!/bin/bash 

module load hadoop 

#export PYSPARK_PYTHON=python3 

spark-submit \
    --master yarn \
    --num-executors 200 \
	--executor-memory 14g \
	main.py
