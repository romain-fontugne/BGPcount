import sys
from subprocess import Popen, PIPE
import glob
import radix
import cPickle as pickle

def readupdates(files, rtree=radix.Radix()):

    p0 = Popen(["bzcat"]+files, stdout=PIPE, bufsize=-1)
    p1 = Popen(["bgpdump", "-m", "-v", "-"], stdin=p0.stdout, stdout=PIPE, bufsize=-1)

    counter = {"W": 0, "A":0}
    for line in p1.stdout:
      
        res = line.split('|',15)
        counter[res[2]] += 1
        if (res[2] == "W"): 
            continue
        
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

    return rtree, counter

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

    print "%s Announces, %s Withdraws, %s unique prefixes" % (counter["A"], counter["W"], len(rtree.nodes())) 
    
