import sys
import numpy as np
from subprocess import Popen, PIPE
from collections import defaultdict
import glob

import networkx as nx
import radix

def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i+n]



class bgpCounter(object):

    def __init__(self):
        self.rtree = radix.Radix()
        self.peerAS = defaultdict(set)
        self.asgraph = nx.Graph()
        self.ribfiles = []
        self.updatefiles = []

    def __getNode(self, prefix):
        """Return the corresponding node in the routing table (and create the entry
        if it does not exists)
        """
	node = self.rtree.search_exact(prefix)
	if node is None:
            node = self.rtree.add(prefix)
            node.data["peerCount"] = defaultdict(int) 
            node.data["origAS"] = set()
                
        return node



    def read_rib(self, files):
        """Read RIB files and populate the routing table and AS graph.
        """

        for f in glob.glob(files):
            self.ribfiles.append(f)
            p1 = Popen(["bgpdump", "-m", "-v", "-t", "change", f], stdout=PIPE, bufsize=-1)

            for line in p1.stdout: 
                res = line.split('|',15)
                zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = res
                
                if zPfx == "0.0.0.0/0":
                    continue

                path = sPath.split(" ")

                # update routing table
		node = self.__getNode(zPfx)
                node.data["peerCount"][zOrig] = 0
                self.peerAS[zOrig].add(zAS)
                node.data["origAS"].add(path[-1])

                # update AS graph
                for a0, a1 in zip(path[:-1], path[1:]):
                    self.asgraph.add_edge(a0,a1)


    def read_update(self, files):
        """Read UPDATE files and count the number of message per peer. 
        """
        
        for f in chunks(glob.glob(files), 96):
            #self.updatefiles.append(f)
            if f[0].endswith("bz2"):
                p0 = Popen(["bzcat"]+f, stdout=PIPE, bufsize=-1)
            else:
                p0 = Popen(["gzcat"]+f, stdout=PIPE, bufsize=-1)
	    p1 = Popen(["bgpdump", "-m", "-v", "-"],  stdin=p0.stdout, stdout=PIPE, bufsize=-1)

	    for line in p1.stdout:
		res = line[:-1].split('|',15)

		if res[5] == "0.0.0.0/0":
		    continue
		
                # update routing table
		node = self.__getNode(res[5])

                zOrig = res[3]
                zAS = res[4]
		if res[2] == "W":
		    pass 

		else:
                    sPath = res[6]
		    #zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = res

		    path = sPath.split(" ")
                    node.data["origAS"].add(path[-1])

                    # update AS graph
                    for a0, a1 in zip(path[:-1], path[1:]):
                        self.asgraph.add_edge(a0,a1)

                node.data["peerCount"][zOrig] += 1
                self.peerAS[zOrig].add(zAS)



    def save_graph(self, filename):
        nx.write_adjlist(self.asgraph, filename)

