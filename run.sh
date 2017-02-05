#!/usr/bin/env bash

#/usr/local/spark/bin/spark-submit sflow-analysis.py local[4] "sflow-analysis" >  out_tech.py

#runs sflow-analysis.py as "sflow-analysis" locally using 4 threads 
python3.5 sflow-analysis.py local[4] "sflow-analysis" >  out_tech.py
