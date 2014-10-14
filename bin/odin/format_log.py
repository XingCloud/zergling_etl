import sys
import datetime
import os
import commands

browsers = ["Opera", "Chrome", "Firefox", "Safari", "MSIE"]
project_short = { "isearch.omiga-plus.com": "omiga-plus",
            "istart.webssearches.com": "webssearches",
            "www.22find.com": "22find",
            "www.istartsurf.com": "istartsurf",
            "www.mystartsearch.com": "mystartsearch",
            "www.sweet-page.com": "sweet-page",
            "www.v9.com": "v9"}

#13 hours
US_TIMEDELTA = datetime.timedelta(hours=-13)

def get_ua(ua):
    for browser in browsers:
        if ua.find(browser) >= 0:
            return browser
    return "Other"

def to_local_time(date):
    return (date + US_TIMEDELTA).strftime("%Y-%m-%dT%H:%M:%S")

def hive_exec(sql):
    os.system('ssh dmnode1 hive -e \"%s\"' % sql)


def load2hdfs(day,logtype,filename):
    hdfs_path = 'odin/%s/%s'%(logtype,day)
    (status, output) = commands.getstatusoutput('hadoop fs -test -d %s' % hdfs_path)
    if status != 0:
        os.system('hadoop fs -mkdir %s' % hdfs_path)
    (status, output) = commands.getstatusoutput('hadoop fs -put %s %s'%(filename,hdfs_path))
    if status == 0:
        print "load %s to hdfs success"% filename
    else:
        print "load %s to hdfs fail"% filename
#nav search log
def parse_search_line(line):
    try:
        attrs = line.split("] ")

        kv = {}
        for attr in attrs:
            index = attr.find("[")
            if index > 0:
                kv[attr[:index]] = attr[index+1:]

        if len(kv["http_referer"]) == 0:
            return None

        reqID = None
        uid = None
        for r in kv["query_string"].split("&"):
            if r.startswith("reqID"):
                reqID = r[6:]

        for c in kv["cookies"].split("; "):
            if c.startswith("uid="):
                uid = c[4:]
                break

        ua = ''
        if "ua" in kv:
            ua = get_ua(kv["ua"])

        query = kv["query"].replace("\t","")

        #format for hive to use
        s_time = datetime.datetime.fromtimestamp(float(kv["ts"])).strftime("%Y-%m-%dT%H:%M:%S")
        nation = kv["country"]
        if reqID and uid and query and len(nation) == 2:
            #p time reqID uid nation ua keyword
            return "%s\t%s\t%s\t%s\t%s\t%s\t%s"%(kv["project_id"],s_time,reqID,uid,nation,ua,query)

    except Exception,e:
        print e

    return None

#nav visit log
def parse_nv_line(line):
    try:
        attrs = line.split("\t")
        time = attrs[2][:19]
        ua = get_ua(attrs[3])
        refer = attrs[4]

        project = refer[7:]
        index = project.find("/")
        if index > 0:
            project = project[0:index]
        if project in project_short:
            pid = project_short[project]
        elif project.endswith("v9.com"):
            #v9 has multi domain ,eg: br.v9.com
            pid = "v9"
        else:
            return None

        params = {}
        for param in attrs[5][8:].split("&"):
            kv = param.split("=")
            params[kv[0]] = kv[1]

        nation = params["User_nation"]
        if len(nation) != 2:
            if attrs[1] == '-':
                return None
            nation = attrs[1].lower()

        #p time reqid uid ip nation ua os width height refer
        return "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"%(pid,time,params["reqID"],params["User_id"],attrs[0],
                nation,ua,params["os"],params["Screen_width"],params["Screen_Height"],refer)

    except Exception,e:
        print e

    return None

def parse_adimp_line(line):
    try:
        attrs = line.split("\t")
        if attrs[0].startswith("2014"): #201410091101
            time = datetime.datetime.strftime(attrs[0],"%Y%m%d%H%M%S")
            attrs[0] = to_local_time(time)
        else: #timestamp
            attrs[0] = datetime.datetime.fromtimestamp(float(attrs[0])).strftime("%Y-%m-%dT%H:%M:%S")
        #switch the uid and reqid
        uid = attrs[1]
        attrs[1] = attrs[2]
        attrs[2] = uid

        return "\t".join(attrs)
    except Exception,e:
        print e

    return None

def parse_file(logtype, source_file, output_file, append=False):
    if append:
        output = open(output_file,"a")
    else:
        output = open(output_file,"w")
    with open(source_file) as f:
        for line in f:
            if line.find("reqID") > 0:
                fmt_line = None
                if logtype == "search":
                    fmt_line = parse_search_line(line.strip())
                elif logtype == "nv":
                    fmt_line = parse_nv_line(line.strip())
                elif logtype == "ad_imp":
                    fmt_line = parse_adimp_line(line.strip())
                if fmt_line:
                    output.write(fmt_line + "\n")
    output.close()

def parse_search_file():
    print "begin at %s" % datetime.datetime.now()
    parent_path = "/data1/user_log/search"
    logtype = "search"
    projects = {
        "isearch.omiga-plus.com": "search",
        "istart.webssearches.com": "search",
        "www.22find.com": "22find",
        "www.istartsurf.com": "istartsurf",
        "www.mystartsearch.com": "mystartsearch",
        "www.sweet-page.com": "sweet-page",
        "www.v9.com": "v9"}

    yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")
    for (project, prefix) in projects.items():
        filename = "%s/%s/%s.log.%s" % (parent_path, project, prefix, yesterday)
        print "format %s at %s" % (filename, datetime.datetime.now())
        parse_file(logtype, filename, filename+"_fmt")
        load2hdfs(yesterday, logtype, filename +"_fmt")
    print "end at %s" % datetime.datetime.now()

    hive_exec("use odin;alter table search add partition(day='%s') location '/user/hadoop/odin/%s/%s/'" % (yesterday, logtype, yesterday))

def parse_nv_file():
    print "begin at %s" % datetime.datetime.now()
    yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")
    path = "/data1/user_log/nv/%s"%yesterday
    logtype = "nv"
    files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and f.endswith("log")]
    for filename in files:
        print "format %s at %s" % (filename, datetime.datetime.now())
        parse_file(logtype, filename, filename +"_fmt")
        load2hdfs(yesterday, logtype, filename +"_fmt")
    print "end at %s" % datetime.datetime.now()
    hive_exec("use odin;alter table nav_visit add partition(day='%s') location '/user/hadoop/odin/%s/%s/'" % (yesterday,logtype,yesterday))

def parse_adimp_file():
    print "begin at %s" % datetime.datetime.now()

    yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")
    output_file = "/data1/user_log/odin/%s.log"%yesterday
    logtype = "ad_imp"

    servers = ["odin0","odin1"]
    for server in servers:
        path = "/data1/user_log/%s/%s"%(server, yesterday)
        files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and f.endswith("log")]
        for filename in files:
            print "format %s at %s" % (filename, datetime.datetime.now())
            parse_file(logtype, filename, output_file, True)
            load2hdfs(yesterday, logtype, output_file)
    print "end at %s" % datetime.datetime.now()
    hive_exec("use odin;alter table ad_impression add partition(day='%s') location '/user/hadoop/odin/%s/%s/'" % (yesterday, logtype, yesterday))

if __name__ == '__main__':
    if len(sys.argv) == 3 and "all" == sys.argv[2]: #daily job
        if sys.argv[1] == "search":
            parse_search_file()
        elif sys.argv[1] == "nv":
            parse_nv_file()
        elif sys.argv[1] == "ad_imp":
            parse_adimp_file()
    elif len(sys.argv) == 4:
        #inputfile outputfile
        parse_file(sys.argv[1],sys.argv[2],sys.argv[3])

    else:
        print "Usage: type inputfilename outputfilename"
        sys.exit(-1)




