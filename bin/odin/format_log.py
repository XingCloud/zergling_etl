# -*- coding:utf-8 -*-

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
US_TIMEDELTA = datetime.timedelta(hours=13)

expired_day = (datetime.datetime.now() + datetime.timedelta(days=-10)).strftime("%Y%m%d")

def get_ua(ua):
    for browser in browsers:
        if ua.find(browser) >= 0:
            return browser
    return "Other"

def to_local_time(date):
    return (date + US_TIMEDELTA).strftime("%Y-%m-%d %H:%M:%S")

def hive_exec(sql):
    os.system("ssh dmnode1 'hive -e \"%s\"" % sql)


def load2hdfs(day,logtype,filename):
    hdfs_path = 'odin/%s/%s'%(logtype,day)
    (status, output) = commands.getstatusoutput('hadoop fs -test -d %s' % hdfs_path)
    if status != 0:
        os.system('hadoop fs -mkdir %s' % hdfs_path)
    #todo remove if exist?
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
            return None,None

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
        s_time = datetime.datetime.fromtimestamp(float(kv["ts"])).strftime("%Y-%m-%d %H:%M:%S")
        nation = kv["country"]
        if reqID and uid and query and len(nation) == 2:
            #p time reqID uid nation ua keyword
            day = s_time[:4] + s_time[5:7] + s_time[8:10]
            return day, "%s\t%s\t%s\t%s\t%s\t%s\t%s"%(kv["project_id"],s_time,reqID,uid,nation,ua,query)

    except Exception,e:
        print e

    return None,None

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
            return None,None

        params = {}
        for param in attrs[5][8:].split("&"):
            kv = param.split("=")
            params[kv[0]] = kv[1]

        nation = params["User_nation"]
        if len(nation) != 2:
            if attrs[1] == '-':
                return None,None
            nation = attrs[1].lower()

        #p time reqid uid ip nation ua os width height refer
        day = time[:4] + time[5:7] + time[8:10]
        return day, "%s\t%s %s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"%(pid, time[:10],time[11:],params["reqID"],params["User_id"],attrs[0],
                nation,ua,params["os"],params["Screen_width"],params["Screen_Height"],refer)

    except Exception,e:
        print e

    return None,None

def parse_adimp_line(line):
    try:
        attrs = line.split("\t")
        if attrs[0].startswith("2014"): #201410091101
            time = datetime.datetime.strptime(attrs[0],"%Y%m%d%H%M%S")
            attrs[0] = to_local_time(time)
        else: #timestamp
            attrs[0] = datetime.datetime.fromtimestamp(float(attrs[0])).strftime("%Y-%m-%d %H:%M:%S")
        #switch the uid and reqid
        uid = attrs[1]
        attrs[1] = attrs[2]
        attrs[2] = uid
        day = attrs[0][:4] + attrs[0][5:7] + attrs[0][8:10]
        return day,"\t".join(attrs)
    except Exception,e:
        print e

    return None,None

def parse_file(logtype, source_file, output_files, single_file=None):
    output_writers = {}

    if single_file:
        output_writers["0000"] = open(single_file, "w")
    else:
        for (day,filename) in output_files.items():
            output_writers[day] = open(filename, "a")

    with open(source_file) as f:
        for line in f:
            fmt_line = None
            if line.find("reqID") > 0: #search and visit must contain reqID
                if logtype == "search":
                    day, fmt_line = parse_search_line(line.strip())
                elif logtype == "nv":
                    day, fmt_line = parse_nv_line(line.strip())
            if logtype == "ad_imp":
                day, fmt_line = parse_adimp_line(line.strip())
            if fmt_line:
                try:
                    if single_file:
                        day = "0000"
                    if day in output_writers:
                        output_writers[day].write(fmt_line + "\n")
                    else:
                        print "Can not find writer for %s"%day
                except Exception,e:
                    print e

    for writer in output_writers.values():
        writer.close()

def parse_search_file(yesterday, today):
    print "begin at %s" % datetime.datetime.now()
    parent_path = "/data1/user_log/search"
    outputpath = "/data1/user_log/search/format";

    logtype = "search"
    projects = {
        "isearch.omiga-plus.com": "search",
        "istart.webssearches.com": "search",
        "www.22find.com": "22find",
        "www.istartsurf.com": "istartsurf",
        "www.mystartsearch.com": "mystartsearch",
        "www.sweet-page.com": "sweet-page",
        "www.v9.com": "v9"}

    for (project, prefix) in projects.items():
        filename = "%s/%s/%s.log.%s" % (parent_path, project, prefix, yesterday)

        output_files = {}
        output_files[yesterday] = "%s/%s_%s.log"%(outputpath, project_short[project], yesterday)
        output_files[today] = "%s/%s_%s.log"%(outputpath, project_short[project], today)

        print "format %s at %s" % (filename, datetime.datetime.now())
        parse_file(logtype, filename, output_files)
        load2hdfs(yesterday, logtype, output_files[yesterday])
    print "end at %s" % datetime.datetime.now()

    print "clean up"
    os.system("rm %s/%s.log" % (outputpath, expired_day))
    for (project, prefix) in projects.items():
        os.system(" rm %s/%s/%s.log.%s" % (parent_path, project, prefix, expired_day))

def parse_nv_file(yesterday, today):
    print "begin at %s" % datetime.datetime.now()

    inputpath = "/data1/user_log/nv/%s"%yesterday
    outputpath = "/data1/user_log/nv/format/"

    output_files = {}
    output_files[yesterday] = outputpath + yesterday + ".log"
    output_files[today] = outputpath + today + ".log"

    logtype = "nv"
    files = [os.path.join(inputpath, f) for f in os.listdir(inputpath) if os.path.isfile(os.path.join(inputpath, f)) and f.endswith("log")]
    for filename in files:
        print "format %s at %s" % (filename, datetime.datetime.now())
        parse_file(logtype, filename, output_files)
        load2hdfs(yesterday, logtype, output_files[yesterday],)
    print "end at %s" % datetime.datetime.now()

    print "clean up"
    os.system("rm /data1/user_log/nv/format/%s.log" % expired_day)
    os.system("rm -rf /data1/user_log/nv/%s/" % expired_day)

def parse_adimp_file(yesterday, today):
    print "begin at %s" % datetime.datetime.now()
    outputpath = "/data1/user_log/odin/"
    logtype = "ad_imp"

    output_files = {}
    output_files[yesterday] = outputpath + yesterday + ".log"
    output_files[today] = outputpath + today + ".log"

    servers = ["odin0","odin1"]
    for server in servers:
        path = "/data1/user_log/%s/%s"%(server, yesterday)
        files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f)) and f.endswith("log")]
        for filename in files:
            print "format %s at %s" % (filename, datetime.datetime.now())
            parse_file(logtype, filename, output_files)
    load2hdfs(yesterday, logtype, output_files[yesterday])
    print "end at %s" % datetime.datetime.now()

    print "clean up"
    for server in servers:
        os.system("rm -rf /data1/user_log/%s/%s"%(server, expired_day))
    os.system("rm %s%s.log"%(outputpath, expired_day))

if __name__ == '__main__':
    if len(sys.argv) >= 3 and "all" == sys.argv[2]: #daily job
        if len(sys.argv) == 4:
            yesterday = sys.argv[3]
            today = (datetime.datetime.strptime(yesterday,"%Y%m%d") + datetime.timedelta(days=1)).strftime("%Y%m%d")
        else:
            yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")
            today = datetime.datetime.now() .strftime("%Y%m%d")

        if sys.argv[1] == "search":
            parse_search_file(yesterday, today)
        elif sys.argv[1] == "nv":
            parse_nv_file(yesterday, today)
        elif sys.argv[1] == "ad_imp":
            parse_adimp_file(yesterday, today)
    elif len(sys.argv) == 5 and "single" == sys.argv[2]:
        parse_file(sys.argv[1], sys.argv[3], None, sys.argv[4])
    else:
        print "Usage: type[search|nv|ad_imp] all (day)"
        sys.exit(-1)




