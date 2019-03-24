from py2neo import Graph, Node, Relationship
from py2neo import Watch

watch("httpstream")

# graph = Graph()
graph = Graph("http://localhost:7474/db/data/",  user = "neo4j", password = "23rdMARDIV")

# transactionContainer
register = graph.begin()

alice = Node("Friend", name="Alice", age=33)
bob = Node("Friend", name="Bob", age=44)

alice["age"] = 33
bob["age"] = 44

register.create(alice)
register.create(bob)

alice_knows_bob = Relationship(alice, "KNOWS", bob)
register.create(alice_knows_bob)

print (alice_knows_bob)

register.commit()

print(graph.exists(alice_knows_bob))

# graph.delete_all()