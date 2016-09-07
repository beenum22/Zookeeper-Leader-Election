from kazoo.client import KazooClient
from kazoo.client import KazooState
import time

def my_listener(state):		#just listening to the state of connection
	if state == KazooState.LOST:
        	print "Connection Lost/Closed"
	elif state == KazooState.SUSPENDED:
        	print "Connection Suspended"
	else:
        	print "Welcome to the Zookeeper"

def with_zeros(node,length):	# while converting the node ids to integar, the leading zeros disappears. They are being added back here
	zeros_node=str(node).zfill(length)
	return zeros_node

def previous(children,mynode,length):	# Find the next lowest node to the current node
	new_children=pop_leader_node(children)
	children1=map(int, new_children)	# Converting values to integar as I was getting an error while using max()
	if int(mynode)==min(children1):
		print "You are the smallest '%s' so become the leader now!!" %mynode
		p_node=mynode
	else:
		my_node=max(t for t in children1 if t<int(mynode))
		print "Current node id is %s " %mynode
		p_node=with_zeros(my_node,length)
		print "Previous node id is %s" %p_node
	return p_node

def smallest(length):	# To find the smallest node 
	children=zk.get_children("/leader")
	children_int= map(int, children)
#	print "children in smallest %s" %children_int
	node=min(int(s) for s in children_int)
	s_node=with_zeros(node,length)	
#	print "Smallest node id is %s" %s_node
	return s_node

def pop_leader_node(children):	#to discard the leader_node temporarily for for processing
	if zk.exists("/leader/leader_node"):
		pop_i=children.index('leader_node')
		del children[pop_i]
	else:
		pass
	return children

def check(event):	#the is the function that will be called when the watch is triggered
	leader(mynode, children, length)

def leader(mynode, children, length):	
	p_node=previous(children,mynode,length)	# Let's see what the next smallest node is	
	if zk.exists("/leader/leader_node"):    # Check to see if the leader node exists
		data,stat=zk.get("/leader/leader_node") #extract the information about current leader
		print "Current Leader is %s" %data
	else:   # If no leader node is present
		s_node=smallest(length)        # Find smallest node in the list
		if int(mynode)==int(s_node):    # If our node is the smallest
			print "Hey you are the leader!! %s" %mynode
			zk.create("/leader/leader_node", mynode.encode('utf8'), ephemeral= True)        #as our node is the leader so become one
		else:	# Our node is not the smallest so just move towards the watch part
			pass
	zk.exists("/leader/%s"%p_node, watch=check)

zk = KazooClient(hosts='172.31.13.20:2181')
zk.add_listener(my_listener)
zk.start()

if zk.exists("/leader"):
	first=0		# assigning this number just to find out if this is first time node is entering the leader election or not
	print "leader node is present"

	
	while True:
		
		if first==0:
			mynode=zk.create("/leader/", "", sequence= True, ephemeral= True)[8:]	#Creates a node for itself
			length=len(mynode)
			children=zk.get_children("/leader")
			leader(mynode, children, length)
			first=first+1
		else:
			children=zk.get_children("/leader")
			length=len(mynode)
			p_node=previous(children,mynode,length)
			print p_node			
			
			print " Watching the next smaller node"

		time.sleep(5)

else:
	print "No leader election"
	
zk.stop()


