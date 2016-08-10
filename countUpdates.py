import sys
from subprocess import Popen, PIPE
import glob
import radix
import cPickle as pickle
import pandas as pd
from matplotlib import pylab as plt

def readupdates(files, rtree=radix.Radix(), prefix=None):

    p0 = Popen(["bzcat"]+files, stdout=PIPE, bufsize=-1)
    p1 = Popen(["bgpdump", "-m", "-v", "-"], stdin=p0.stdout, stdout=PIPE, bufsize=-1)

    counter = {"W": [], "A":[], "ts":[]}
    for line in p1.stdout:
      
        res = line.split('|',15)
        if (res[2] == "W"): 
            # For the plot
            if prefix is None :
                counter["W"].append(1)
                counter["A"].append(0)
                counter["ts"].append(zDt)
            elif zPfx == "prefix":
                counter["W"].append(1)
                counter["A"].append(0)
                counter["ts"].append(zDt)
        
        elif (res[2] == "A"):
            zTd, zDt, zS, zOrig, zAS, zPfx, sPath, zPro, zOr, z0, z1, z2, z3, z4, z5 = res

            if rtree.search_exact(zPfx) is None:
                node = rtree.add(zPfx)
                node.data["firsttime"] = zDt
                node.data["lasttime"] = zDt
                node.data["nbUpdate"] = 1
                node.data["path"] = sPath 

            else :
                node = rtree.search_exact(zPfx)
                node.data["lasttime"] = zDt
                node.data["path"] = sPath 
                node.data["nbUpdate"] += 1
            
            # For the plot
            if prefix is None :
                counter["A"].append(1)
                counter["W"].append(0)
                counter["ts"].append(zDt)
            elif zPfx == prefix:
                counter["A"].append(1)
                counter["W"].append(0)
                counter["ts"].append(zDt)

    return rtree, pd.DataFrame(counter)

if __name__ == "__main__":

    if len(sys.argv) < 2:
        print("usage: %s updatefiles*.bz2" % sys.argv[0])
        sys.exit()
        
    # list update files
    files = glob.glob(sys.argv[1])
    if len(files)==0:
        print("Files not found!")
        sys.exit()

    files.sort()

    rtree, counter = readupdates(files)
    print "%s Announces, %s Withdraws, %s unique prefixes" % (counter["A"].sum(), counter["W"].sum(), len(rtree.nodes())) 

    counter.index = pd.to_datetime(counter["ts"], unit="s")
    counter.drop("ts", 1)
    counter1h = counter.resample("1H").sum()

    plt.figure()
    plt.plot(counter1h.index, counter1h["A"], label="updates")
    # plt.plot(counter1h.index, counter1h["W"], label="withdrawals")
    plt.ylabel("#Updates per hour")
    plt.xticks(rotation=70)
    plt.tight_layout()
    plt.savefig("nbUpdates.eps")
    
