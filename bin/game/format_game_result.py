import sys

def parseFile(path):
    data = [] #dict can not sort,use string
    with open(path) as f:
        for line in f:
            kv = line.strip().split("\t")
            data.append("\"%s\":%s"%(kv[0],kv[1]))
    return ",".join(data)

def parseData(date):
    mtpath = "/data0/game/%s_mt.log"%(date)
    mppath = "/data0/game/%s_mp.log"%(date)
    mt = parseFile(mtpath)
    mp = parseFile(mppath)

    resultpath = "/data0/game/%s_fmt.log"%(date)
    with open(resultpath, "w") as f:
        f.write("{\"mt\":{%s},\"mp\":{%s}}"%(mt,mp))

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print "usage: date"
        exit(-1)

    date = sys.argv[1]
    parseData(date)