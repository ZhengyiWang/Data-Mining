# -*- coding: utf-8 -*-

from pyspark.context import SparkContext
import time
import sys

start_time=time.time()
sc=SparkContext("local[*]","task1")
case= int(sys.argv[1])
supports= int(sys.argv[2])
input_file=sc.textFile(sys.argv[3]) 

header=input_file.first()
input_file=input_file.filter(lambda input_file: input_file!=header)

rdd=input_file.map(lambda data: data.split(",")).map(lambda a:(a[0],a[1])).distinct().persist()

def create_baskets(rdd,case) :
	if case==1:
		res= rdd.map(lambda a:(a[0],[a[1]])).reduceByKey(lambda a,b:a+b).map(lambda a:a[1])
	else:
		res= rdd.map(lambda a:(a[1], a[0])).map(lambda a:(a[0],[a[1]])).reduceByKey(lambda a,b:a+b).map(lambda a:a[1])
	return res

baskets= create_baskets(rdd,case)
baskets_list= baskets.collect()

def item_list(basket_list):
    item=set()
    
    for basket in basket_list:
       item=item.union(set(basket))
    item=list(item)
    
    return list(map(lambda x:tuple([x]),item))

item=item_list(baskets_list) 

#A-priori
def keys_filter(basket_list, keys_list,support):
    keys_dic={}
    for k in keys_list:
        count = 0
        for basket in basket_list:
            if set(k)<=set(basket):
                count=count+1
            if count>=support:
                keys_dic[k]=count
                break
    return keys_dic


def apriori_gen(keys1,k):
    keys2 = dict()
    for i in range(len(keys1)):
        for j in range(i+1,len(keys1)):
            if(set(keys1[i][0:k-1]) == set(keys1[j][0:k-1])) :
                tmp=list(set(keys1[i])|set(keys1[j]))
                tmp.sort()
                tmp=tuple(tmp)
                if (keys2.get(tmp,True)):
                    keys2[tmp]=False
                else:
                    break
    return list(keys2.keys())


def a_priori(b,item,support):
    basket_list=list(b)
    support_threshold=support*(len(basket_list)/len(baskets_list))
    f_items=keys_filter(basket_list,item,support_threshold)
    F=[list(f_items.keys())]
    k=1
    while F[k-1]!=[]:
        keys=apriori_gen(F[k-1],k)
        f_items=keys_filter(basket_list,keys,support_threshold)
        F.append(list(f_items.keys()))
        k=k+1
        
    U =[]
    for f in F:
        for x in f:
            U.append(x)
    return U

# start SON algorithm


# SON algorithm phase 1
SON_Phase1= baskets.mapPartitions(lambda data: a_priori(data,item,supports)).map(lambda k: (k,1))
Candidates= SON_Phase1.distinct().map(lambda a: a[0]).collect()

w_1={}
for can in Candidates:
    if len(can) not in w_1.keys():
        w_1[len(can)]=[can]
    else:
        w_1[len(can)].append(can)

w_1_list=[]
for value in w_1.values():
    value.sort()
    w_1_list.append(value)

#SON algorithm phase 2

def count_SONphase2(basket_list, key_list) :
	basket=list(basket_list)
	key_count=list()

	for k in key_list :
		count=0
		for b in basket :
			if (set(k)<=set(b)) :
				count=count+1
		key_count.append([k,count])

	return key_count

#SON algorithm PHASE 2
SON_Phase2= baskets.mapPartitions(lambda data: count_SONphase2(data, Candidates))
Frequent_Itemsets= SON_Phase2.reduceByKey(lambda a,b: (a+b)).filter(lambda a: a[1]>=supports).sortByKey().collect()

Frequent_Itemsets=sorted(Frequent_Itemsets, key=lambda t: len(t[0]))
Frequent_Itemsets=sorted(Frequent_Itemsets, key=lambda t:t)

w_2={}
for fre in Frequent_Itemsets:
    if len(fre[0]) not in w_2.keys():
        w_2[len(fre[0])]=[fre[0]]
    else:
        w_2[len(fre[0])].append(fre[0])

w_2_list=[]
for value in w_2.values():
    value.sort()
    w_2_list.append(value)

with open(sys.argv[4],"w") as f:          
   f.write("Candidates:"+"\n")
   f.write(str(w_1_list[0][0])[:-2]+")")
   for ite in w_1_list[0][1:]:
       f.write(","+str(ite)[:-2]+")")
   
   for w in w_1_list[1:]:
       f.write("\n\n")
       f.write(str(w[0]))
       for ite in w[1:]:
           if ite:
               f.write(","+str(ite))

   f.write("\n\n")
   f.write("Frequent Itemsets:"+"\n")
   f.write(str(w_1_list[0][0])[:-2]+")")
   for ite in w_2_list[0][1:]:
       f.write(","+str(ite)[:-2]+")")
   
   for w in w_2_list[1:]:
       f.write("\n\n")
       f.write(str(w[0]))
       for ite in w[1:]:
           if ite:
               f.write(","+str(ite)) 

            
end_time=time.time()
print("Duration:",end_time-start_time)