#!/usr/bin/env bash
#runs sflow-analysis.py as "sflow-analysis" locally using 4 threads to analyse data from test_sample.csv file 

#via spark-submit
spark-submit sflow-analysis.py local[4] "sflow-analysis" "test_sample.csv" >  out_tech.txt

#or via python  
sflow-analysis.py local[4] "sflow-analysis" "test_sample.csv" >  out_tech.txt
