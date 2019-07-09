# -*- coding: utf-8 -*-

from pyspark import SparkContext
import sys
import json

sc = SparkContext( "local[*]", "task1")
input_file=sc.textFile(sys.argv[1])

#create the main rdd
rdd=input_file.map(json.loads) \
    .map(lambda data: (data["useful"],data["stars"],data["text"],data["user_id"],data["business_id"])).persist()


#A. The number of reviews that people think are useful (The value of tag ‘useful’ > 0)
useful_count= rdd.filter(lambda data: data[0]>0).count()

#B. The number of reviews that have 5.0 stars rating 
rating_count= rdd.filter(lambda data: data[1]==5).count()

#C. How many characters are there in the ‘text’ of the longest review
longest_characters=rdd.map(lambda data: len(data[2])).max()

#D. The number of distinct users who wrote reviews 
user_list= rdd.map(lambda data: (data[3])).persist()

user_count= user_list.distinct().count()

#E. The top 20 users who wrote the largest numbers of reviews and the number of reviews they wrote 
user_top= user_list.map(lambda data: (data, 1)).reduceByKey(lambda a,b: a+b) \
        .sortByKey(ascending=True).map(lambda a:(a[1],a[0])) \
        .sortByKey(ascending=False).map(lambda a:(a[1],a[0])).take(20)


user_json= str(user_top).replace('(','[').replace(')',']').replace('\'','\"')

#F. The number of distinct businesses that have been reviewed 
business_list= rdd.map(lambda data: (data[4])).persist()

business_count= business_list.distinct().count()

#G. The top 20 businesses that had the largest numbers of reviews and the number of reviews they had
business_top= business_list.map(lambda data: (data,1)).reduceByKey(lambda a,b: a+b) \
        .sortByKey(ascending=True).map(lambda a:(a[1],a[0])) \
        .sortByKey(ascending=False).map(lambda a: (a[1],a[0])).take(20)

business_json= str(business_top).replace('(','[').replace(')',']').replace('\'','\"')

#write into the file
with open(sys.argv[2], "w") as f:
    f.write("{\n")
    f.write("\"useful_count\":"+str(useful_count)+",\n")
    f.write("\"5_stars_rating\":"+str(rating_count)+",\n")
    f.write("\"longest_characters\":"+str(longest_characters)+",\n")
    f.write("\"distinct_users\":"+str(user_count)+",\n")
    f.write("\"top_20_users\":"+user_json+",\n")
    f.write("\"distinct_business\":"+str(business_count)+",\n")
    f.write("\"top20_business\":"+business_json+"\n")
    f.write("}")
    