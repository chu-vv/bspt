#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 20 15:34:39 2017

@author: V
"""
import time #to measure time
import json #to convert output to json format
import sys
from pyspark import SparkContext, SparkConf

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from collections import OrderedDict

sampling_rate=512

countr_out=open('out_count_list.txt','w')

# function to define country by IP using geolite2 database
# function takes (<ip>,<packet size>) and returns (<country name>,<packet size>)
def f(iter):
    from geoip2 import database
    reader = database.Reader("GeoLite2-Country.mmdb")
    try:
        country = reader.country(iter[0]).country.name
    except:
        country ='NOT_FOUND'
    return country,iter[1]

#strict configuration
#sc = SparkContext("local[4]", "sflow-data-analysis")

#to configure with shell parameters
sc= SparkContext(sys.argv[1],sys.argv[2])

#computations start time
comp_start=time.time()

data=sc.textFile(sys.argv[3])#("sflow-0118.csv")  #n  full test data sample dispatcher.netpoint-dc.com
#data=sc.textFile("one.csv")   #small sample of sflow data to debug
#data=sc.textFile("large.csv")   #6gb sample


#columns=from original rdd get rdd of tuples (<ip>,<packet size>) and reducing it by ip
reduc=data.map(lambda x: x.split(',')). \
	flatMap(lambda y: [(y[10],int(y[18])),(y[11],int(y[18]))]). \
	map(lambda x: (x[0],x[1]*sampling_rate)). \
	reduceByKey(lambda x,y: x+y) #n

res_dict=OrderedDict(reduc.collect())
print('sizeofres',sys.getsizeof(res_dict))
### total amount of traffic per country computation
countrdd_reduc=reduc.map(f).reduceByKey(lambda x,y: x+y) #n 

#export results from rdd sorted by amount of traffic to ordered dictionary
count_dic=OrderedDict(countrdd_reduc.sortBy(lambda x: -x[1]).collect())
print('sizeofcount',sys.getsizeof(count_dic))
comp_fin=time.time()
print('computations complete in',comp_fin-comp_start)

###write traffic per country as json

#with open('out_country.txt','w') as outcountry:
#        json.dump(count_dic,outcountry)

#and as a list of json-type records
countr_out=open('out_count_list.txt','w')
countr_out.write('[ \n')
tostr=''
i=0
n=len(count_dic)
for key in count_dic:
	tostr='{"country": '+json.dumps(key)+',"sum":'+str(count_dic[key])+'}'
	if(i<n-1):
		countr_out.write(tostr+',\n')
	else:   
		countr_out.write(tostr+'\n')
	i+=1

countr_out.write(']')	
###end countries

### diagram construction. The diagram is saved to countries.png file in current directory
plt.figure(figsize=(len(count_dic)*0.8,10))
plt.autoscale(enable=True,axis='x',tight=True)
plt.bar(range(len(count_dic)),count_dic.values(), width=0.8, align='center',log=True)
plt.xticks(range(len(count_dic)), count_dic.keys(), rotation='vertical')
plt.xlabel('country')
plt.ylabel('traffic, b')
plt.savefig('countries.png',bbox_inches='tight')
###end diagram

###write traffic per IP (res_dict) as json

#with open('out.txt','w') as output:
#	json.dump(res_dict,output)


#and as a list of json-type records
res_list=reduc.collect()
ip_out=open('out_ip_list.txt','w')
ip_out.write('[ \n')
i=0
n=len(res_dict)
for key in res_dict:
	tostr='{"ip":'+json.dumps(key)+',"sum":'+str(res_dict[key])+'}' 
	if(i<n-1):
		ip_out.write(tostr+',\n')
	else:
		ip_out.write(tostr+'\n')
	i+=1

ip_out.write(']')

print ('write results complete in', time.time()-comp_fin,'; total time is', time.time()-comp_start)
