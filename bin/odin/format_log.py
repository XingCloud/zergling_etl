# -*- coding:utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import datetime
import os
import commands
import urllib
import re
import urlparse
import json

browsers = {"Opera":"Opera", "Chrome":"Chrome", "Firefox":"Firefox", "Safari":"Safari", "MSIE":"MSIE", "Trident":"MSIE"}
project_short = { "isearch.omiga-plus.com": "omiga-plus",
                  "istart.webssearches.com": "webssearches",
                  "www.22find.com": "22find",
                  "www.istartsurf.com": "istartsurf",
                  "www.mystartsearch.com": "mystartsearch",
                  "www.sweet-page.com": "sweet-page",
                  "www.v9.com": "v9",
                  "www.delta-homes.com":"delta-homes",
                  "www.key-find.com": "key-find",
                  "myhome.vi-view.com":"vi-view",
                  "www.omniboxes.com":"omniboxes",
                  "www.luckysearches.com":"luckysearches",
                  "www.luckybeginning.com":"luckybeginning",
                  "www.do-search.com":"do-search",
                  "mystart.uninstallmaster.com":"uninstallmaster",
                  "mystart.vi-view.com":"vi-view",
                  "www.qone8.com":"qone8",
                  "mystart.live-hotspot.com":"live-hotspot"
}

expired_day = (datetime.datetime.now() + datetime.timedelta(days=-5)).strftime("%Y%m%d")
this = __import__(__name__)

def get_browser(ua):
    for (browser_type, name) in browsers.items():
        if ua.find(browser_type) >= 0:
            return name
    return "Other"

def check_uid(uid):
    return re.match(r'.*[;,:%!@#$%^&*()-=+\[\]\{\}"\'<>\\\.].*' , uid)

def hive_exec(sql):
    os.system("ssh dmnode1 'hive -e \"%s\"" % sql)

def getIP(strIp):
    ips = strIp.split(",")
    if len(ips) >1:
        return ips[len(ips)-1].strip()
    return strIp

def mergeAndLoad(yesterday,logtype,orig_filename,tdby_output_file,yesterday_filename):

    #merge orig date to natural day
    stand_yesterday = "%s-%s-%s"%(yesterday[:4], yesterday[4:6], yesterday[6:])

    if os.path.isfile(tdby_output_file): #first time does not have this file
        print "grep '%s ' %s > %s"%(stand_yesterday, tdby_output_file, yesterday_filename)
        os.system("grep '%s ' %s > %s"%(stand_yesterday, tdby_output_file, yesterday_filename))

    print "grep '%s ' %s >> %s"%(stand_yesterday, orig_filename, yesterday_filename)
    os.system("grep '%s ' %s >> %s"%(stand_yesterday, orig_filename, yesterday_filename))

    hdfs_path = 'odin/%s/%s'%(logtype,yesterday)
    (status, output) = commands.getstatusoutput('hadoop fs -test -d %s' % hdfs_path)
    if status != 0:
        os.system('hadoop fs -mkdir %s' % hdfs_path)

    (status, output) = commands.getstatusoutput('hadoop fs -test -e %s/%s' % (hdfs_path, os.path.basename(yesterday_filename)))
    if status == 0:
        os.system('hadoop fs -rm %s/%s' % (hdfs_path, os.path.basename(yesterday_filename)))
    #todo remove if exist?
    (status, output) = commands.getstatusoutput('hadoop fs -put %s %s'%(yesterday_filename,hdfs_path))
    if status == 0:
        print "load %s to hdfs success"% yesterday_filename
    else:
        print "load %s to hdfs fail"% yesterday_filename

#nav search log
def parse_search_line(line):
    try:
        if line.find("reqID") < 0:
            return None

        attrs = line.split("] ")

        kv = {}
        for attr in attrs:
            index = attr.find("[")
            if index > 0:
                kv[attr[:index]] = attr[index+1:]

        if len(kv["http_referer"]) == 0: return None

        reqID = None
        uid = None
        for r in kv["query_string"].split("&"):
            if r.startswith("reqID"): reqID = r[6:]

        for c in kv["cookies"].split("; "):
            if c.startswith("uid="):
                uid = c[4:]
                break

        ua = 'Other'
        if "ua" in kv:
            ua = get_browser(kv["ua"])

        query = kv["query"].replace("\t","")

        #format for hive to use
        s_time = datetime.datetime.fromtimestamp(float(kv["ts"])).strftime("%Y-%m-%d %H:%M:%S")
        nation = kv["country"]
        if reqID and uid and check_uid(uid) and query and len(nation) == 2:
            #p time reqID uid nation ua keyword
            return "%s\t%s\t%s\t%s\t%s\t%s\t%s"%(kv["project_id"],s_time,reqID,uid,nation,ua,query)

    except Exception,e:
        print e

    return None

#nav visit log
def parse_nv_line(line):
    try:
        if line.find("reqID") < 0:
            return None

        attrs = line.split("\t")
        time = attrs[2][:19]
        ua = get_browser(attrs[3])
        refer = attrs[4]

        refer_params = {}
        index = refer.find("?")
        if index > 0 :
            for param in refer[index+1:].split("&"):
                kv = param.split("=")
                refer_params[kv[0]] = kv[1]

        ref = '\N'
        if "from" in refer_params:
            ref = refer_params["from"]

        opt = '\N'
        if "type" in refer_params:
            opt = refer_params["type"]

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

        if not check_uid(params["User_id"]):
            print params["User_id"]
            return None

        nation = attrs[1].lower()
        if nation == '-':
            nation = params["User_nation"]
        if len(nation) != 2:
            return None
        ip = getIP(attrs[0])

        #p time reqid uid ip nation ua os width height refer
        return "%s\t%s %s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"%(pid, time[:10],time[11:],params["reqID"],params["User_id"],ip,
                                                                        nation,ua,params["os"],params["Screen_width"],params["Screen_Height"],'\N',ref,opt)

    except Exception,e:
        print e

    return None

def parse_adimp_line(line):
    try:
        attrs = line.split("\t")
        if attrs[6] not in project_short.values() or not "10004" == attrs[3] or 'NA' == attrs[2]:
            return None

        attrs[0] = datetime.datetime.fromtimestamp(float(attrs[0])).strftime("%Y-%m-%d %H:%M:%S")

        return "\t".join(attrs)
    except Exception,e:
        print e

    return None

def parse_gdp_line(line):
    try:
        attrs = line.split("\t")
        ip = attrs[0].split(",")[0]
        time = attrs[1][:19]
        nation = attrs[2].lower()

        params = {}
        for param in attrs[3][9:].split("&"):
            index = param.find("=")
            params[param[:index]] = param[index+1:]

        title = urllib.unquote(params["title"])
        meta = urllib.unquote(params["meta"])
        url = urllib.unquote(params["url"])

        #time uid ip nation lang os ptid title meta url
        return "%s %s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"%(time[:10],time[11:],params["uid"],ip,nation,
                                                            params["lang"],params["os"],params["ptid"],title,meta,url)
    except Exception,e:
        print e

    return None

def parse_ac_line(line):
    try:
        attrs = line.split("\t")
        time = attrs[1][:19]
        params = {}
        for param in attrs[2][8:].split("&"):
            index = param.find("=")
            params[param[:index]] = param[index+1:]
        reqid = params["reqid"]

        nation = '\N'
        uid = '\N'

        if len(attrs) == 5:
            nation = attrs[4].lower()
            params = {}
            for param in attrs[3][attrs[3].find("?")+1:].split("&"):
                index = param.find("=")
                params[param[:index]] = param[index+1:]
            uid = params["uid"]

        #http://www.sweet-page.com/?type=hp&ts=1414075222&from=cor&uid=ST500DM002-1BD142_Z3TP5QG4        FR

        #time reqid
        return "%s %s\t%s\t%s\t%s"%(time[:10],time[11:],reqid,uid, nation)
    except Exception,e:
        print e
    return None

def parse_ad_feimp_line(line):
    try:
        attrs = line.split("\t")
        time = attrs[1][:19]
        params = {}
        for param in attrs[3][9:].split("&"):
            index = param.find("=")
            params[param[:index]] = param[index+1:]
        reqid = params["req_id"]

        if 'NA' == reqid:
            return None

        nation = attrs[2].lower()
        if nation == '-':
            nation = '\N'

        adid = "\N"
        if "code" in params:
            adid = params["code"]

        slot = "10004"
        if "slot" in params:
            slot = params["slot"]

        dt = "\N"
        if "dt" in params:
            dt = params["dt"]

        #time reqid nation
        return "%s %s\t%s\t%s\t%s\t%s\t%s"%(time[:10],time[11:],reqid,nation,adid,slot,dt)
    except Exception,e:
        print e
    return None

def parse_ares_line(line):
    try:
        attrs = line.split("\"")
        ip = getIP(attrs[0].split(" ")[0])
        t = attrs[0].split("[")[1][:-2]
        url_str = attrs[1][4:-9]
        ft = (datetime.datetime.strptime(t, "%a, %d %b %Y %H:%M:%S GMT") + datetime.timedelta(hours=8))
        sft = ft.strftime("%Y-%m-%d %H:%M:%S")
        url = urlparse.urlparse(url_str)
        params = urlparse.parse_qs(url.query, False)

        site = '\N'
        pid = '\N'
        if "site" in params :
            site = params["site"][0]
            pid = site.split(".")[1]

        browser = get_browser(attrs[-2])

        category = '\N'
        if "category" in params and "undefined" != params["category"][0]:
            category = params["category"][0]

        nation = params["nation"][0].lower()
        if len(nation) != 2:
            nation = '\N'

        lang = params["language"][0]
        if len(lang) > 5:
            lang = '\N'

        hit = "\N"
        if "hit" in params:
            hit = params["hit"][0]

        #uid reqid ip nation size category language adid camp_id site slot cookie browser time

        return "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (params["uid"][0],
                                                                               params["reqid"][0], ip, nation, params["size"][0], category,
                                                                               lang, params["adid"][0], params["camp_id"][0], site, pid,
                                                                               params["slot"][0], params["cookie"][0], browser, sft, hit)

    except Exception, e:
        print e
    return None

def parse_ares_click_line(line):
    try:
        click = json.loads(line)
        time = datetime.datetime.fromtimestamp(float(str(click["ts"])[:10])).strftime("%Y-%m-%d %H:%M:%S")
        uid = getFieldValue(click, "uid")
        reqid = getFieldValue(click, "reqid")
        camp_id = click["id"]
        ad_id = getFieldValue(click, "ad")
        ip = getFieldValue(click, "ip")
        ts = click["ts"]
        clickid = click["clickid"]
        nation = click["nation"]
        if not nation or len(nation) != 2:
            nation  = '\N'
        else:
            nation.lower()
        site = getFieldValue(click, "site")
        if site != '\N' and site != "blank" :
            pid = site.split(".")[1]
        else:
            pid = getFieldValue(click, "src")

        monkey = click["monkey"]
        sub1 = getFieldValue(click, "sub1")
        sub2 = getFieldValue(click, "sub2")
        network = getFieldValue(click, "network")
        category = click["category"]
        title = getFieldValue(click, "title")

        #uid reqid camp_id ad_id ip time ts clickid nation site pid monkey sub1 sub2 network category title
        return "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s" % (uid,
                                                                                       reqid, camp_id, ad_id, ip, time, ts, clickid, nation, site, pid, monkey,
                                                                                       sub1, sub2, network, category, title)
    except Exception, e:
        print e
        print line
    return None

def getFieldValue(obj, attrName):
    if attrName in obj and obj[attrName]:
        value = obj[attrName].strip()
        if len(value) == 0:
            return '\N'
        return value
    else:
        return '\N'

def parse_file(parser, source_file, output_file, mode="w"):
    if not os.path.isfile(source_file):
        print "%s not exsit"%source_file
        return

    output_writer = open(output_file, mode)
    with open(source_file) as f:
        for line in f:
            fmt_line = getattr(this, parser)(line.strip())
            if fmt_line:
                output_writer.write(fmt_line + "\n")
    output_writer.close()

def parse_search_file(yesterday, tdb_yesterday):
    print "begin at %s" % datetime.datetime.now()
    parent_path = "/data1/user_log/search"
    outputpath = "/data1/user_log/search/format"

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

        output_file = "%s/%s_%s_orig.log"%(outputpath, project_short[project], yesterday)
        tdby_output_file = "%s/%s_%s_orig.log"%(outputpath, project_short[project], tdb_yesterday)

        print "format %s at %s" % (filename, datetime.datetime.now())
        parse_file('parse_search_line', filename, output_file)
        mergeAndLoad(yesterday, logtype, output_file, tdby_output_file, "%s/%s_%s.log"%(outputpath,project_short[project], yesterday))
    print "end at %s" % datetime.datetime.now()

    print "clean up"
    os.system("rm -f %s/%s.log" % (outputpath, expired_day))
    for (project, prefix) in projects.items():
        os.system(" rm -f %s/%s/%s.log.%s" % (parent_path, project, prefix, expired_day))

def parse_default_file(logtype,yesterday, tdb_yesterday):
    print "begin at %s" % datetime.datetime.now()
    parentpath = "/data1/user_log/%s"%logtype
    inputpath = "%s/%s"%(parentpath,yesterday)
    outputpath = "%s/format/"%parentpath
    output_file = outputpath + yesterday + "_orig.log"
    tdby_output_file = outputpath + tdb_yesterday + "_orig.log"

    files = sorted([os.path.join(inputpath, f) for f in os.listdir(inputpath) if os.path.isfile(os.path.join(inputpath, f)) and f.endswith("log")])
    mode = "w"
    for filename in files:
        print "format %s at %s" % (filename, datetime.datetime.now())
        parse_file("parse_%s_line"%logtype, filename, output_file,mode)
        mode = "a"

    mergeAndLoad(yesterday, logtype, output_file, tdby_output_file, outputpath + yesterday + ".log")
    print "end at %s" % datetime.datetime.now()

    print "clean up"
    os.system("rm -f %s/format/%s*.log" % (parentpath, expired_day))
    os.system("rm -rf %s/%s/" % (parentpath, expired_day))

def parse_adimp_file(yesterday, tdb_yesterday):
    print "begin at %s" % datetime.datetime.now()
    outputpath = "/data1/user_log/odin/"
    logtype = "ad_imp"

    output_file = outputpath + yesterday + "_orig.log"
    tdby_output_file = outputpath + tdb_yesterday + "_orig.log"

    servers = ["odin0","odin1"]
    mode = "w"  # cover the output file at beginning.
    for server in servers:
        path = "/data1/user_log/%s/%s"%(server, yesterday)
        files = sorted([os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))
                                                                           and f.startswith("odin-%s"%yesterday) and f.endswith("log")])
        for filename in files:
            print "format %s at %s" % (filename, datetime.datetime.now())
            parse_file('parse_adimp_line', filename, output_file, mode)
            mode = "a"
    mergeAndLoad(yesterday, logtype, output_file,tdby_output_file,outputpath + yesterday + ".log")
    print "end at %s" % datetime.datetime.now()

    print "clean up"
    for server in servers:
        os.system("rm -rf /data1/user_log/%s/%s"%(server, expired_day))
    os.system("rm %s%s*.log"%(outputpath, expired_day))

if __name__ == '__main__':
    if len(sys.argv) >= 3 and "all" == sys.argv[2]: #daily job
        if len(sys.argv) == 4:
            yesterday = sys.argv[3]
            tdb_yesterday = (datetime.datetime.strptime(yesterday,"%Y%m%d") + datetime.timedelta(days=-1)).strftime("%Y%m%d")
        else:
            yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")
            tdb_yesterday = (datetime.datetime.now() + datetime.timedelta(days=-2)).strftime("%Y%m%d")

        if sys.argv[1] == "search":
            parse_search_file(yesterday, tdb_yesterday)
        elif sys.argv[1] == "ad_imp":
            parse_adimp_file(yesterday, tdb_yesterday)
        elif sys.argv[1] == "nv" or sys.argv[1] == "gdp" or sys.argv[1] == "ac" or sys.argv[1] == "ad_feimp" \
            or sys.argv[1] == "ares" or sys.argv[1] == "ares_click":
            parse_default_file(sys.argv[1], yesterday, tdb_yesterday)
    elif len(sys.argv) == 5 and "single" == sys.argv[2]:
        func = None
        if sys.argv[1] == "search":
            func = parse_search_line
        elif sys.argv[1] == "nv":
            func = parse_nv_line
        elif sys.argv[1] == "ad_imp":
            func = parse_adimp_line
        elif sys.argv[1] == "gdp":
            func = parse_gdp_line
        if func:
            parse_file(func, sys.argv[3], sys.argv[4])
    else:
        print "Usage: type[search|nv|ad_imp|gdp|ares] all (day)"
        sys.exit(-1)
