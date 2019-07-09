import sys
from pyspark import SparkContext
import time
import itertools

sc = SparkContext("local[*]","task1")

filter_threshold= int(sys.argv[1])

input_file= sc.textFile(sys.argv[2])
input_file= input_file.map(lambda data: data.split(','))
input_rdd= input_file.filter(lambda data: data[0]!= "user_id").persist()

business_user_baskets=input_rdd.map(lambda data: (data[1], [data[0]])).reduceByKey(lambda x,y: x+y)

def generate_edges(g):
	business_id=g[0]
	user_ids=g[1]
	res=[]

	edges= itertools.combinations(user_ids, 2)
	for e in edges:
		e=sorted(e)
		res.append(((e[0],e[1]), [business_id]))

	return res

edges_lists= business_user_baskets.flatMap(lambda x: generate_edges(x)).reduceByKey(lambda x,y: x+y) \
  .filter(lambda x: len(x[1])>=filter_threshold).map(lambda x: x[0])

vertices_rdds= edges_lists.flatMap(lambda x: [(x[0]),(x[1])]).distinct()

edges_lists=edges_lists.collect()

adjacency_lists={}
for v in edges_lists:
	if v[0] in adjacency_lists.keys():
		 adjacency_lists[v[0]].append(v[1])
	elif v[0] not in adjacency_lists.keys():
		adjacency_lists[v[0]]=[v[1]]
	if v[1] in adjacency_lists.keys():
		 adjacency_lists[v[1]].append(v[0])
	elif v[1] not in adjacency_lists.keys():
		adjacency_lists[v[1]]=[v[0]]	    

	
def calculate_betweenness(root_node, adjacency_list):

	q= []
	visited= []
	level= {}
	parent= {}
	weight= {}

	q.append(root_node)
	visited.append(root_node)
	level[root_node]= 0
	weight[root_node]= 1
	
	while q:
		node= q.pop(0)
		children= adjacency_list[node]

		for v in children:
			if(v not in visited):
				q.append(v)
				parent[v]= [node]
				weight[v]= weight[node]
				visited.append(v)
				level[v]= level[node]+1
			else:
				if(v!=root_node) and (level[node]==level[v]-1):
					parent[v].append(node)	
					weight[v]+= weight[node]

	
	reverse_visited=[]
	for i in range(len(visited)):
		reverse_visited.append(visited[len(visited)-1-i])
	
	nodes_credit= {}
	for i in reverse_visited:
		nodes_credit[i]= 1
	
	betweenness_value={}

	for c in reverse_visited:
		if (c!=root_node):
			total_weight= 0
			for p in parent[c]:
					total_weight=total_weight+weight[p]

			for p in parent[c]:
				k=tuple(sorted([c,p]))

				if(k not in betweenness_value.keys()):
					betweenness_value[k]=nodes_credit[c]*weight[p]/total_weight
				else:
					betweenness_value[k]+=nodes_credit[c]*weight[p]/total_weight

				nodes_credit[p]+= nodes_credit[c]*weight[p]/total_weight
	return betweenness_value

def merge_dict(x,y):
	for k,v in x.items():
		if k in y.keys():
			y[k]+=v
		else:
			y[k]=v
	return y	


res={}
for v in adjacency_lists.keys():
	res1=calculate_betweenness(v,adjacency_lists)
	res=merge_dict(res1,res)

write_file=[]
for k,v in res.items():
	write_file.append((k,v/2))

write_file=sorted(write_file,key=(lambda x:x[0]))
write_file= sorted(write_file,key=(lambda x: x[1]),reverse=True)


with open(sys.argv[3], "w") as f:
	f.write(str(write_file[0][0])+", "+str(write_file[0][1]))
	for i in write_file[1:]:
			f.write("\n"+str(i[0])+", "+str(i[1]))


"""community Detection"""
def is_empty(remaind_graph):
	if(len(remaind_graph)==0):
		return False
	else:
		for k in remaind_graph.keys():
			adj_list= remaind_graph[k]
			if(len(adj_list)!=0):
				return True
			
		return False

def create_component(root_node, adjacency_list):
	q=[]
	vertex=[]
	edge= []

	q.append(root_node)
	vertex.append(root_node)

	while q:
		node= q.pop(0)
		children= adjacency_list[node]

		for i in children:
			if(i not in vertex):
				q.append(i)
				vertex.append(i)

			e= sorted((node,i))
			if(e not in edge):
				edge.append(e)

	return (vertex,edge)

def remove_component(remaind_graph, component):
	remove_vertex=component[0]

	for v in remove_vertex:
		del remaind_graph[v]

	for i in remaind_graph.keys():
		adj_list= remaind_graph[i]
		for v in remove_vertex:
			if(v in adj_list):
				adj_list.remove(v)
		remaind_graph[i]= adj_list		

	return remaind_graph

def remove_edge(adjacency_matrix, first_edge_to_remove):
	if(first_edge_to_remove[0] in adjacency_matrix.keys()):
		l=adjacency_matrix[first_edge_to_remove[0]]
		if(first_edge_to_remove[1] in set(l)):
			l.remove(first_edge_to_remove[1])

	if(first_edge_to_remove[1] in adjacency_matrix.keys()):
		l= adjacency_matrix[first_edge_to_remove[1]]
		if(first_edge_to_remove[0] in set(l)):
			l.remove(first_edge_to_remove[0])
	
	return adjacency_matrix

def find_component(adjacency_matrix):
	all_component= []
	remaind_graph= adjacency_matrix

	while is_empty(remaind_graph):
		root=list(remaind_graph.keys())[0]
		
		component= create_component(root, adjacency_matrix)
		all_component.append(component)
		remaind_graph= remove_component(remaind_graph, component)

	return all_component
	
def calculate_modularity(all_component):
	modularity= 0
	for c in all_component:
		c_vertices= c[0]

		Aij=0
		for i in c_vertices:
			for j in c_vertices:
				Aij=0
				adj_list= adjacency_lists[i]
				if(j in adj_list):
					Aij=1

				ki= len(adjacency_lists[i])
				kj= len(adjacency_lists[j])

				modularity+= Aij-(ki*kj)/(2*m)	
	modularity= modularity/(2*m)

	return modularity

def create_adjacency_list(all_component):
	ad_list= {}
	for c in all_component:
		c_edges= c[1]

		for ce in c_edges:
			if(ce[0] in ad_list.keys()):
				ad_list[ce[0]].append(ce[1])
			else:
				ad_list[ce[0]]= [ce[1]]

			if(ce[1] in ad_list.keys()):
				ad_list[ce[1]].append(ce[0])
			else:
				ad_list[ce[1]]= [ce[0]]
	
	return ad_list

first_edge_to_remove= write_file[0][0]
m=len(edges_lists)

adjacency_matrixs= adjacency_lists.copy()

highest_modularity= -1
communities=[]
count=0

while count<len(edges_lists)-1:	
	adjacency_matrixs= remove_edge(adjacency_matrixs, first_edge_to_remove)
	all_components= find_component(adjacency_matrixs)
	modularity= calculate_modularity(all_components)

	adjacency_matrixs= create_adjacency_list(all_components)
	
	res={}
	for v in adjacency_matrixs.keys():
		res1=calculate_betweenness(v,adjacency_matrixs)
		res=merge_dict(res1,res)
	
	write_file=[]
	for k,v in res.items():
		write_file.append((k,v/2))
	
	write_file=sorted(write_file,key=(lambda x:x[1]),reverse=True)
	
	first_edge_to_remove= write_file[0][0]
	if(modularity>=highest_modularity):
		highest_modularity= modularity
		communities= all_components
	count+=1


sorted_communities= []
for c in communities :
	item= sorted(c[0])
	sorted_communities.append(item)

sorted_communities=sorted(sorted_communities, key=lambda x:x[0])
sorted_communities=sorted(sorted_communities,key=lambda x:len(x))


with open(sys.argv[4],"w") as f:
	s=str(sorted_communities[0]).replace("[","").replace("]","")
	f.write(s)
	for i in sorted_communities[1:]:
		s=str(i).replace("[","").replace("]","")
		f.write("\n"+s)