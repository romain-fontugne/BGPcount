import sys
import os
import cPickle as pickle
import logging
import numpy as np
from collections import defaultdict

import bgpCounter


def find_noisiest_prefixes(bc, top=100):
    nodes = bc.rtree.nodes()
    totalCount = np.zeros(len(nodes))

    for n, node in enumerate(nodes):
        totalCount[n] = np.sum(node.data["peerCount"].values()) 

    ind = np.argpartition(totalCount, -top)[-top:]
    noisyind = ind[np.argsort(totalCount[ind])]

    for i in noisyind:
        peercount = nodes[i].data["peerCount"]
        uniqAS = set([ asn for asnSet in bc.peerAS.values() for asn in asnSet])
        ascount = defaultdict(list) 

        for peerIP, count in peercount.iteritems():
            asList = list(bc.peerAS[peerIP])
            try:
                asList.remove("0")
            except ValueError:
                pass
            asn = asList[0]
            if len(asList) > 1:
                logging.warning("Peer IP (%s) corresponds to more than one AS (%s)" % (peerIP, asList))
            ascount[asn].append(count)

        fi = open("noisiestPrefixes/%s_AS%s_msg%s.txt" % (nodes[i].prefix.replace("/","_"), "_".join(list(nodes[i].data["origAS"])), totalCount[i] ), "w")
        for asn, count in ascount.iteritems(): 
            fi.write("%s\t%s\n" % (asn, count))

        fi.close()


if __name__ == "__main__":

    pickleFile = "saved_bc_afterUPDATE.pickle"
    if os.path.exists(pickleFile):
        logging.info("Load data from pickle")
        bc =pickle.load(open(pickleFile,"rb"))

    else:
        bc = bgpCounter.bgpCounter()
    
        logging.info("Read RV RIB files...")
        bc.read_rib("/data/routeviews/archive.routeviews.org/*/bgpdata/2016.06/RIBS/rib.20160601.0000.bz2")
        logging.info("Read RIS RIB files...")
        bc.read_rib("/data/ris/*/2016.06/bview.20160601.0000.gz")

        pickle.dump(bc, open("saved_bc_afterRIB.pickle","wb"),protocol=2)

        logging.info("Read RV UPDATE files...")
        bc.read_update("/data/routeviews/archive.routeviews.org/*/bgpdata/2016.06/UPDATES/updates.20160601.*.bz2")
        logging.info("Read RIS UPDATE files...")
        bc.read_update("/data/ris/*/2016.06/updates.20160601.*.gz")

        pickle.dump(bc, open(pickleFile,"wb"),protocol=2)

    #bc.save_graph("AS_graph.txt")
    logging.info("Finding noisiest prefixes...")
    find_noisiest_prefixes(bc)
