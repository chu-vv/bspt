#!/usr/bin/env bash
#runs sflow-analysis.py as "sflow-analysis" locally using 4 threads to analyse data from sflow-0118.csv file 

#via spark-submit
spark-submit sflow-analysis.py local[4] "sflow-analysis" "sflow-0118.csv" >  out_tech.txt

#or via python  
sflow-analysis.py local[4] "sflow-analysis" "sflow-0118.csv" >  out_tech.txt
