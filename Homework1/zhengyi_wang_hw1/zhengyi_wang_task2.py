# -*- coding: utf-8 -*-
from pyspark import SparkContext
import sys
import json
import time

sc = SparkContext( "local[*]", "task2")

review_file=sc.textFile(sys.argv[1])
business_file=sc.textFile(sys.argv[2])

#A The average starts for each state
review_rdd=review_file.map(json.loads).map(lambda data:(data["business_id"],data["stars"])).persist()
business_rdd=business_file.map(json.loads).map(lambda data:(data["business_id"],data["state"])).persist()

#(business_id, (state, stars)) -> (state,(stars,1))
business_review=business_rdd.join(review_rdd).map(lambda data: (data[1][0], (data[1][1],1)))

state_avg=business_review.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])).map(lambda a:(a[0], a[1][0]/a[1][1])) \
    .sortByKey(ascending=True).map(lambda a:(a[1],a[0])) \
    .sortByKey(ascending=False).map(lambda a:(a[1],a[0]))

result=state_avg.collect()

with open(sys.argv[3], "w") as f:
    f.write("state,stars\n")
    for item in result:
        f.write(item[0]+","+str(item[1])+"\n")

#B Two methods of print Top 5 states
start_1=time.time()
result_1=state_avg.collect()
for i in range(5):
    print(result_1[i])
end_1=time.time()
Method_1_time=end_1-start_1

start_2=time.time()
result_2=state_avg.take(5)
print(result_2)
end_2=time.time()
Method_2_time=end_2-start_2

with open(sys.argv[4],"w") as f1:
    f1.write("{\n")
    f1.write("\"m1\":"+str(Method_1_time)+",\n")
    f1.write("\"m2\":"+str(Method_2_time)+",\n")
    f1.write("\"explanation\":"+"\"For method 1, you need to transfer all the data from the RDD to list, \
but for method 2, the operation take(5) let the computer only need transfer the top 5 data from the RDD to list. \
Because the second method need to handle less data than the first method, \
the Method 2 spends less time than Method 1.\""+"\n" )
    f1.write("}")








