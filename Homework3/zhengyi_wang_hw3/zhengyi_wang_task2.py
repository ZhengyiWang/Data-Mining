import sys
from pyspark import SparkContext
import time
from pyspark.mllib.recommendation import ALS, Rating
import math

start_time=time.time()
sc=SparkContext("local[*]","task2")


train_data= sc.textFile(sys.argv[1])
train_data= train_data.map(lambda x : x.split(','))
train_rdds= train_data.filter(lambda x: x[0]!= "user_id").persist()

test_data= sc.textFile(sys.argv[2])
test_data= test_data.map(lambda x : x.split(','))
test_rdds= test_data.filter(lambda x: x[0]!= "user_id").persist()

case_id=int(sys.argv[3])

def case1(train_rdd,test_rdd):
    full_rdd=sc.union([train_rdd,test_rdd])

    users=full_rdd.map(lambda a:a[0]).distinct().collect()
    businesses=full_rdd.map(lambda a:a[1]).distinct().collect()
    
    users_dict={}
    for u in range(len(users)):
        users_dict[users[u]]=u
    
    users_dict_re={v: k for k, v in users_dict.items()}
    
    businesses_dict={}
    for b in range(len(businesses)):
        businesses_dict[businesses[b]]=b
        
    businesses_dict_re={v: k for k, v in businesses_dict.items()}
    
    train_ratings=train_rdd.map(lambda x: Rating(int(users_dict[x[0]]), int(businesses_dict[x[1]]), float(x[2])))
    test_ratings= test_rdd.map(lambda x: Rating(int(users_dict[x[0]]), int(businesses_dict[x[1]]), float(x[2])))
    
    
    model=ALS.train(train_ratings,5,5,0.1)
    
    testdata = test_ratings.map(lambda x: (x[0], x[1]))
    
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2])).collectAsMap()
    
    testdata=testdata.collect()
    
    with open(sys.argv[4],"w") as f:
            f.write("user_id, business_id, prediction")
            for k in testdata:
                f.write("\n"+users_dict_re[k[0]]+","+businesses_dict_re[k[1]]+","+str(predictions.get(k,3.7)))

def case2(train_rdd,test_rdd):

    test_datasets=test_rdd.map(lambda x:(x[0],x[1]))
    users_businesses_baskets=train_rdd.map(lambda a:(a[0],[a[1]])).reduceByKey(lambda x,y:x+y)
    users_ratings_baskets= train_rdd.map(lambda a:(a[0],[float(a[2])])).reduceByKey(lambda x,y:x+y)
    businesses_users_baskets=train_rdd.map(lambda a: (a[1],[a[0]])).reduceByKey(lambda x,y: x+y)
    users_bura_dict=users_businesses_baskets.join(users_ratings_baskets).map(lambda a:(a[0], dict(zip(a[1][0],a[1][1]))))
    
    businesses_users_baskets=businesses_users_baskets.collectAsMap()
    ubr_dict=users_bura_dict.collectAsMap()
    
    
    def avg(bu_set,ou):
        return sum([ubr_dict[ou][bu] for bu in bu_set])/len(bu_set)
    
    def normalizing(bu_set,t_avgrating, o_avgrating,tuser,u):
        t_normrating=[]
        o_normrating=[]
        for b in bu_set:
            t_normrating.append(ubr_dict[tuser][b]-t_avgrating)
            o_normrating.append(ubr_dict[u][b]-o_avgrating)
        return t_normrating,o_normrating
    
    def cal_coeffience(t_normrating,o_normrating):
        numerato=sum([t_normrating[i]*o_normrating[i] for i in range(len(t_normrating))]) 
        denominator=math.sqrt(sum([i*i for i in t_normrating]))*math.sqrt(sum([j*j for j in o_normrating]))
        if denominator!=0:
            return  numerato/denominator
        else:
            return 0
    
    def predict_users_ratings(test_dataset):
        tuser=test_dataset[0]
        tbusiness=test_dataset[1]
    
        trated_business=ubr_dict.get(tuser,False)
        if trated_business:
            trated_business=ubr_dict[tuser].keys()
            avg_rating0=avg(trated_business,tuser)
        else:
            return ((tuser,tbusiness),3)
       
        
        if tbusiness not in businesses_users_baskets.keys():
            return ((tuser,tbusiness),avg_rating0)
        else:
            other_rated_users=businesses_users_baskets[tbusiness]
            person_coefficient=[]
            person_rate=[]
            
            for ous in other_rated_users:
                corated_business=set(trated_business)&set(ubr_dict[ous].keys())
                if len(corated_business)==0:
                    continue
                
                t_avgrating=avg_rating0
                o_avgrating=avg(ubr_dict[ous].keys(),ous)        
                
                t_normratings=[]
                o_normratings=[]
                t_normratings,o_normratings=normalizing(corated_business,t_avgrating,o_avgrating,tuser,ous)
                
                coefficient= cal_coeffience(t_normratings,o_normratings)
                person_coefficient.append(coefficient)
                
                avg_rating1=(sum([ubr_dict[ous][b] for b in ubr_dict[ous].keys()])-ubr_dict[ous][tbusiness])/(len(ubr_dict[ous].keys())-1)
                nnn=ubr_dict[ous][tbusiness]-avg_rating1
                person_rate.append(nnn)
            if sum([abs(a) for a in person_coefficient])==0:
                return ((tuser,tbusiness),avg_rating0)
            else:
                pre_rates=avg_rating0+sum([person_rate[i]*person_coefficient[i] for i in range(len(person_rate))])/sum([abs(a) for a in person_coefficient])
                if pre_rates<=1:
                    pre_rates=1
                elif pre_rates>=5:
                    pre_rates=5
                return ((tuser,tbusiness),pre_rates)
    
    res=test_datasets.map(lambda data: predict_users_ratings(data)).collect()
    
    with open(sys.argv[4],"w") as f:
            f.write("user_id, business_id, prediction")
            for k in res:
                f.write("\n"+k[0][0]+","+k[0][1]+","+str(k[1]))

def case3(train_rdd,test_rdd):
    
    test_datasets=test_rdd.map(lambda x:(x[0],x[1]))
    businesses_users_baskets=train_rdd.map(lambda a:(a[1],[a[0]])).reduceByKey(lambda x,y:x+y)
    businesses_ratings_baskets=train_rdd.map(lambda a:(a[1],[float(a[2])])).reduceByKey(lambda x,y:x+y)
    
    users_businesses_baskets=train_rdd.map(lambda a: (a[0],[a[1]])).reduceByKey(lambda x,y: x+y)
    businesses_urra_dict=businesses_users_baskets.join(businesses_ratings_baskets).map(lambda a:(a[0], dict(zip(a[1][0],a[1][1]))))
    
    users_businesses_baskets=users_businesses_baskets.collectAsMap()
    bur_dict=businesses_urra_dict.collectAsMap()
     
    
    def avg(bu,us_set):
        return sum([bur_dict[bu][us] for us in us_set])/len(us_set)
    
    def normalizing(us_set,t_avgrating, o_avgrating,tbusiness,b):
        t_normrating=[]
        o_normrating=[]
        for u in us_set:
            t_normrating.append(bur_dict[tbusiness][u]-t_avgrating)
            o_normrating.append(bur_dict[b][u]-o_avgrating)
        return t_normrating,o_normrating
    
    def cal_coeffience(t_normrating,o_normrating):
        numerato=sum([t_normrating[i]*o_normrating[i] for i in range(len(t_normrating))]) 
        denominator=math.sqrt(sum([i*i for i in t_normrating]))*math.sqrt(sum([j*j for j in o_normrating]))
        if denominator!=0:
            return  numerato/denominator
        else:
            return 0
    
    def predict_items_ratings(test_dataset): 
        tuser=test_dataset[0]
        tbusiness=test_dataset[1]
        
        tratedby_users=bur_dict.get(tbusiness,False)
        if tratedby_users:
            tratedby_users=bur_dict[tbusiness].keys()
            avg_rating0=avg(tbusiness,tratedby_users)
        else:
            return ((tuser,tbusiness),3)
        
        if tuser not in users_businesses_baskets.keys():
            return ((tuser,tbusiness),avg_rating0)
        else:
            other_rated_businesses=users_businesses_baskets[tuser]
            similarities=[]
            
            for obs in other_rated_businesses:
                corated_users=set(tratedby_users)&set(bur_dict[obs].keys())
                if len(corated_users)==0:
                    continue
                
                t_avgrating=avg_rating0
                o_avgrating=avg(obs,bur_dict[obs].keys())
                
                t_normratings=[]
                o_normratings=[]
                t_normratings,o_normratings=normalizing(corated_users,t_avgrating,o_avgrating,tbusiness,obs)
                
                coefficient= cal_coeffience(t_normratings,o_normratings)
                similarities.append((coefficient,bur_dict[obs][tuser]))                
                
            similarities=list(filter(lambda x:x[0]>0.1, similarities))
            
            if sum([abs(sim[0]) for sim in similarities])==0:
                return ((tuser,tbusiness),avg_rating0)
            else:
                pre_rates=sum([sim[0]*sim[1] for sim in similarities])/sum([abs(sim[0]) for sim in similarities])
                if pre_rates<=1:
                    pre_rates=1
                elif pre_rates>=5:
                    pre_rates=5
                return ((tuser,tbusiness),pre_rates)
    
    res=test_datasets.map(lambda data: predict_items_ratings(data)).collect()
    
    with open(sys.argv[4],"w") as f:
            f.write("user_id, business_id, prediction")
            for k in res:
                f.write("\n"+k[0][0]+","+k[0][1]+","+str(k[1]))

if case_id==1:
    case1(train_rdds,test_rdds)
elif case_id==2:
    case2(train_rdds,test_rdds)              
else:
    case3(train_rdds,test_rdds)              

end_time=time.time()
print("Duration:", end_time-start_time)                
    