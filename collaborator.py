import csv
from py2neo import Graph, Node, Relationship, NodeMatcher

def main():

	#Create instance of collaborator db/graph and log in
	graph = Graph("http://localhost:7474/db/data", user = "neo4j", password = "23rdMARDIV")
	#Deletes ALL nodes in the space - not just for this csv
	graph.delete_all()
	
	#load all csvs into db
	users(graph)
	interests(graph)
	organizations(graph)
	projects(graph)
	skills(graph)
	distances(graph)

	user_in = "1"
	matchCommonInterests(graph, user_in)

#Utility for parsing csv
def strip(string): 
	return''.join([c if 0 < ord(c) < 128 else ' ' for c in string]) 
	#removes non utf-8 chars from string within cell

# For a university user, find all other individuals who share the
# same interests or skills as the user, and work in the same or
# different organization within 10 miles from the organization
# that the user works. The individuals should be ranked by the
# total weight of shared interests (or skills) with the user. In
# addition, the output should include the organization name,
# and the list of common interests (or skills).
def matchCommonInterests(graph, user_in):

	matcher = NodeMatcher(graph)
	matcher.match("User", user_id=user_in)
	# MATCH(origin:User {user_id=user_in})-[interest_origin:INTERESTED_IN]->(interest_actual:name)
	# RETURN interest_actual.name, interest_origin.weight

	# <-[interest_terminal:INTERESTED_IN]-(user_terminal:User)

	# if(graph.nodes.match("User", first_name=user_in).first())
	# if(graph.nodes.match("User", user_id=user_in)==None)
	# 	print("User does not exist")
	# else
		# match(user:User)-[:INTERESTED_IN]->(interest)<-[:INTERESTED_IN]-(potential_collaborator)
		# WHERE user.user_id = user_in AND organization.name = organization

#Processes user.csv
def users(graph):

	#Transaction Container - where we store operations before committing to graph
	register = graph.begin()
	#Open file and assign to reader
	fileIn = "user.csv"
	in_file = open(fileIn, 'r')
	reader = csv.reader(in_file, delimiter=',')
   
	try:
		i = 0;
		j = 0;
		next(reader)
		for row in reader:
			if row:
				user = strip(row[0])
				firstName = strip(row[1])
				lastName = strip(row[2])

			userNode = Node("User", user_id=user, first_name=firstName, last_name=lastName)
			#primary key and label used for merge comparison
			userNode.__primarylabel__ = "User"
			userNode.__primarykey__ = "user_id"

			register.create(userNode)

			i += 1
			j += 1

		if (i == 4): #submits a batch every 4 lines read
			register.commit()
			print (j, "distance lines processed")
			i = 0                
			register = graph.begin()
		else: 
			register.commit() #submits remainder of lines read                       
			print (j, "distance lines processed")     

	except Exception as e:
		print (e, row, reader.line_num)

#Processes distance.csv
def distances(graph):

	#Transaction Container - where we store operations before committing to graph
	register = graph.begin()
	#Open file and assign to reader
	fileIn = "distance.csv"
	in_file = open(fileIn, 'r')
	reader = csv.reader(in_file, delimiter=',')
   
	try:
		i = 0;
		j = 0;
		next(reader)
		for row in reader:
			if row:
				organization_1 = strip(row[0])
				organization_2 = strip(row[1])
				dist = strip(row[2])

			organizationNode1 = Node("Organization", name=organization_1)
			organizationNode2 = Node("Organization", name=organization_2)

			DISTANCE = Relationship.type("DISTANCE")
			#Merge combines if already exists, else creates new, using primary label and key
			register.merge(DISTANCE(organizationNode1, organizationNode2), "Organization", "name")

			# register.create(organizationNode1)
			# register.create(organizationNode2)
			# Org_to_Org = Relationship(organizationNode1, "DISTANCE", organizationNode2, distance=dist)
			# register.create(Org_to_Org)
			# print (Org_to_Org)

			i += 1
			j += 1

		if (i == 4): #submits a batch every 4 lines read
			register.commit()
			print (j, "distance lines processed")
			i = 0                
			register = graph.begin()
		else: 
			register.commit() #submits remainder of lines read                       
			print (j, "distance lines processed")     

	except Exception as e:
		print (e, row, reader.line_num)

#Processes interest.csv
def interests(graph):

	#Transaction Container - where we store operations before committing to graph
	register = graph.begin()
	#Open file and assign to reader
	fileIn = "interest.csv"
	in_file = open(fileIn, 'r')
	reader = csv.reader(in_file, delimiter=',')
   
	try:
		i = 0;
		j = 0;
		next(reader)
		for row in reader:
			if row:
				user = strip(row[0])
				interest = strip(row[1])
				interestLevel = strip(row[2])

			userNode = Node("User", user_id=user)
			#primary key and label used for merge comparison
			userNode.__primarylabel__ = "User"
			userNode.__primarykey__ = "user_id"

			interestNode = Node("Interest", name=interest)
			interestNode.__primarylabel__ = "Interest"
			interestNode.__primarykey__ = "name"

			INTERESTED_IN = Relationship.type("INTERESTED_IN")
			#Merge combines if already exists, else creates new
			register.merge(INTERESTED_IN(userNode, interestNode, weight=interestLevel))

			i += 1
			j += 1

		if (i == 4): #submits a batch every 4 lines read
			register.commit()
			print (j, "interest lines processed")
			i = 0                
			register = graph.begin()
		else: 
			register.commit() #submits remainder of lines read                       
			print (j, "interest lines processed")     

	except Exception as e:
		print (e, row, reader.line_num)

#Processes organization.csv
def organizations(graph):

	#Transaction Container - where we store operations before committing to graph
	register = graph.begin()
	#Open file and assign to reader
	fileIn = "organization.csv"
	in_file = open(fileIn, 'r')
	reader = csv.reader(in_file, delimiter=',')
   
	try:
		i = 0;
		j = 0;
		next(reader)
		for row in reader:
			if row:
				user = strip(row[0])
				organization = strip(row[1])
				orgType = strip(row[2])

			userNode = Node("User", user_id=user)
			#primary key and label used for merge comparison
			userNode.__primarylabel__ = "User"
			userNode.__primarykey__ = "user_id"

			organizationNode = Node("Organization", name=organization, type=orgType)
			organizationNode.__primarylabel__ = "Organization"
			organizationNode.__primarykey__ = "name"

			WORKS_FOR = Relationship.type("WORKS_FOR")
			#Merge combines if already exists, else creates new
			register.merge(WORKS_FOR(userNode, organizationNode))
			
			i += 1
			j += 1

		if (i == 4): #submits a batch every 4 lines read
			register.commit()
			print (j, "organization lines processed")
			i = 0                
			register = graph.begin()
		else: 
			register.commit() #submits remainder of lines read                       
			print (j, "organization lines processed")     

	except Exception as e:
		print (e, row, reader.line_num)

#Processes project.csv
def projects(graph):

	#Transaction Container - where we store operations before committing to graph
	register = graph.begin()
	#Open file and assign to reader
	fileIn = "project.csv"
	in_file = open(fileIn, 'r')
	reader = csv.reader(in_file, delimiter=',')
   
	try:
		i = 0;
		j = 0;
		next(reader)
		for row in reader:
			if row:
				user = strip(row[0])
				project = strip(row[1])

			userNode = Node("User", user_id=user)
			#primary key and label used for merge comparison
			userNode.__primarylabel__ = "User"
			userNode.__primarykey__ = "user_id"

			projectNode = Node("Project", name=project)
			projectNode.__primarylabel__ = "Project"
			projectNode.__primarykey__ = "name"

			WORKS_ON = Relationship.type("WORKS_ON")
			#Merge combines if already exists, else creates new
			register.merge(WORKS_ON(userNode, projectNode))
			
			i += 1
			j += 1

		if (i == 4): #submits a batch every 4 lines read
			register.commit()
			print (j, "project lines processed")
			i = 0                
			register = graph.begin()
		else: 
			register.commit() #submits remainder of lines read                       
			print (j, "project lines processed")     

	except Exception as e:
		print (e, row, reader.line_num)

#Processes skill.csv
def skills(graph):

	#Transaction Container - where we store operations before committing to graph
	register = graph.begin()
	#Open file and assign to reader
	fileIn = "skill.csv"
	in_file = open(fileIn, 'r')
	reader = csv.reader(in_file, delimiter=',')

	try:
		i = 0;
		j = 0;
		next(reader)
		for row in reader:
			if row:
				user = strip(row[0])
				skill = strip(row[1])
			
			userNode = Node("User", user_id=user)
			#primary key and label used for merge comparison
			userNode.__primarylabel__ = "User"
			userNode.__primarykey__ = "user_id"

			skillNode = Node("Skill", name=skill)
			skillNode.__primarylabel__ = "Skill"
			skillNode.__primarykey__ = "name"

			SKILLED_IN = Relationship.type("SKILLED_IN")
			#Merge combines if already exists, else creates new
			register.merge(SKILLED_IN(userNode, skillNode))
			
			i += 1
			j += 1

		if (i == 4): #submits a batch every 4 lines read
			register.commit()
			print (j, "skill lines processed")
			i = 0                
			register = graph.begin()
		else: 
			register.commit() #submits remainder of lines read                       
			print (j, "skill lines processed")     

	except Exception as e:
		print (e, row, reader.line_num)


if __name__ == '__main__':
    main()
