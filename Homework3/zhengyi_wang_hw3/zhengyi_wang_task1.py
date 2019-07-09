# -*- coding: utf-8 -*-

import sys
from pyspark import SparkContext
import time
from itertools import combinations
from pyspark import SparkConf

start_time=time.time()

conf=SparkConf()
conf.set("spark.executor.memory","4g")
conf.set("spark driver.memory","4g")



sc=SparkContext("local[*]","task1",conf)
input_file=sc.textFile(sys.argv[1])

header=input_file.first()
input_file=input_file.filter(lambda input_file:input_file!=header)

rdd=input_file.map(lambda data: data.split(",")).persist()

users=rdd.map(lambda a:a[0]).distinct().collect()
businesses=rdd.map(lambda a:a[1]).distinct().collect()

usersid_dict={}
for u in range(len(users)):
	usersid_dict[users[u]]= u
    
"""
       Input matrix
	┆U1┆U2┆…┆Un
┄┄┄
B1┆
B2┆
 …┆
Bn┆

"""
businesses_baskets= rdd.map(lambda a: (a[1],[usersid_dict[a[0]]])).reduceByKey(lambda x,y: x+y).persist()

businesses_users_dict=dict(businesses_baskets.collect())

factor=[7,  11, 13, 17, 19, 23,  29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 
        73, 79, 83, 89, 97, 101,103,107,109,113,127,131,137,139,149,151,157,
        163,167,173,179,181,191,193,197,199,211,223,227,229,233,239,241,251,
        257,263,269,271,277,281,283,293,307,311,313,317,331,347,349,353,359,
        367,373,379,383,389,397,401,409,419,421,431,433,439,443,449,457,461,
        463,467,479,487,491,499,503,509,521,523,541,547,557,563,569,571,577]

"""
       Signature matrix
	┆U1┆U2┆…┆Un
┄┄┄
B1┆
B2┆
 …┆
Bn┆
  Compare rows
"""
def minhashing(matrix):
    minNum = [min((a*x+3)%len(users) for x in matrix[1]) for a in factor]
    return (matrix[0],minNum)

signature_matrix=businesses_baskets.map(lambda x:minhashing(x))

#LSH
rows=2
bands=51

def divide_into_band(row):
    
	business_id= row[0]
	signature_list= row[1]
    
	band_list=[]
    
	tmp1=[signature_list[i] for i in range(len(signature_list)) if i%2==0]
	tmp2=[signature_list[i] for i in range(len(signature_list)) if i%2==1]
	
	for i in range(len(tmp1)):
		band_list.append(((i,tmp1[i],tmp2[i]),[business_id]))

	return band_list

buckets=signature_matrix.flatMap(lambda x: divide_into_band(x)) \
    .reduceByKey(lambda x,y:x+y).filter(lambda x:len(x[1])>1)


def generate_pair(bucket):
    
	business_list= bucket[1]
	
	candidate=list(combinations(business_list,2))
    
	pair=[]
	for c in candidate:
		pair.append((c,1))
        
	return pair

pairs=buckets.flatMap(lambda businesses_list: generate_pair(businesses_list)).distinct()

def calculate_similarity(pair):
	business1= pair[0][0]
	business2= pair[0][1]

	user_list1= set(businesses_users_dict[business1])
	user_list2= set(businesses_users_dict[business2])
	
	sim=len(set(user_list1)&set(user_list2))/len(set(user_list1)|set(user_list2))

	return ((sorted((business1, business2)), sim))

res=pairs.map(lambda a: calculate_similarity(a)).filter(lambda a: a[1]>=0.5).sortByKey().collect()


with open(sys.argv[2],"w") as f:
    f.write("business_id_1, business_id_2, similarity")
    for pair in res:
        f.write("\n"+pair[0][0]+","+pair[0][1]+","+str(pair[1]))

end_time=time.time()
print("Duration:", end_time-start_time)