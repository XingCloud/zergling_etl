import datetime
import sys

#201.162.36.159  2014-07-13T23:55:10+08:00       /gm.png?action=hb&appid=337&uid=uK2C3FPAShRJjgnFBSOWAg6161&gid=sponge_bob_squarepants_anchovy_assault&l=es&ts=1405266916137&tz=5
def parsePayLog(inPath,outPath):
    infile = open(inPath)
    outfile = open(outPath,"w")
    for line in infile:
        attrs = line.split("\t")
        uriParams = attrs[2][8:].split("&")
        params = {}
        for p in uriParams:
            kv = p.split("=")
            if kv[1]:
                params[kv[0]] = kv[1]
        if params.has_key("ts") and params.has_key("uid") and params.has_key("gid") and params.has_key("l"):
            outfile.write("%s\t%s\t%s\t%s\n"%(parseTimeStamp(attrs[1][0:19]),params["uid"],params["gid"],params["l"]))

    outfile.close()
    infile.close()


def parseTimeStamp(timestamp):
    return datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print "usage: inputpath outputpath"
        exit(-1)

    inpath = sys.argv[1]
    outpath = sys.argv[2]

    parsePayLog(inpath,outpath)