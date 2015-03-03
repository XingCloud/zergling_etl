import os
import sys
import datetime
import smtplib
import email.MIMEMultipart
import email.MIMEText
import email.MIMEBase

mailto_list = ["liqiang@xingcloud.com","zhangyi1942@gmail.com","chengen@elex-tech.com","wuzhongju@elex-tech.comv","chenshihua@elex-tech.com"]
mail_host = "smtp.qq.com"
mail_user = "xamonitor@xingcloud.com"
mail_pass = "22C1NziwxZI5F"

sql = '''
insert overwrite local directory '/data1/odin/dayily_count'
row format delimited
fields terminated by ','
select * from (
select visit.pid,visit.pv,visit.pr,visit.pu,se.sv,se.sr,se.su,an.iv,an.ir,an.iu,ic.cv,ic.cr,ic.cu from
(select pid, count(*) pv, count(distinct reqid) pr, count(distinct uid) pu from odin.nav_visit where day='%s' group by pid ) visit left outer join
(select pid, count(*) sv, count(distinct reqid) sr, count(distinct uid) su from odin.search where day='%s' group by pid) se on visit.pid = se.pid left outer join
(select pid, count(*) iv, count(distinct reqid) ir, count(distinct uid) iu from odin.ad_impression where day='%s' group by pid) an on an.pid = se.pid left outer join
(select pid, count(*) cv, count(distinct ai.reqid) cr, count(distinct ai.uid) cu from odin.ad_click ac join odin.ad_impression ai on ai.reqid = ac.reqid and ai.day = ac.day where ac.day = '%s' group by pid) ic on ic.pid = visit.pid
union all
select tv.pid,tv.pv,tv.pr,tv.pu,ts.sv,ts.sr,ts.su,ti.iv,ti.ir,ti.iu,tc.cv,tc.cr,tc.cu from
(select 'total' pid, count(*) pv , count(distinct reqid) pr,count(distinct uid) pu from odin.nav_visit where day='%s') tv join
(select 'total' pid, count(*) sv, count(distinct reqid) sr,count(distinct uid) su from odin.search where day='%s') ts on tv.pid = ts.pid join
(select 'total' pid, count(*) iv, count(distinct reqid) ir, count(distinct uid) iu from odin.ad_impression where day='%s') ti on ti.pid = ts.pid join
(select 'total' pid, count(*) cv, count(distinct reqid) cr, '' cu from odin.ad_click where day='%s') tc on tc.pid = ti.pid ) tmp
'''

def sendMail(subject,content, filename):
    me="xamonitor@xingcloud.com"
    msg = email.MIMEMultipart.MIMEMultipart()
    msg['Subject'] = "odin daily log count " + subject
    msg['From'] = "liqiang@xingcloud.com"
    msg['To'] = ";".join(mailto_list)
    try:
        text_msg = email.MIMEText.MIMEText(content)
        msg.attach(text_msg)

        for f in filename:
            msg.attach(attach_file(f))

        s = smtplib.SMTP()
        s.connect(mail_host)
        s.login(mail_user, mail_pass)
        s.sendmail(me, mailto_list, msg.as_string())
        s.close()
        return True
    except Exception, e:
        print str(e)
        return False

def attach_file(filename):
    contype = 'application/octet-stream'
    maintype, subtype = contype.split('/', 1)
    data = open(filename, 'rb')
    file_msg = email.MIMEBase.MIMEBase(maintype, subtype)
    file_msg.set_payload(data.read())
    data.close()
    email.Encoders.encode_base64(file_msg)

    basename = os.path.basename(filename)
    file_msg.add_header('Content-Disposition','attachment', filename = basename)

    return file_msg

def count_odin(day):
    daily_sql = sql%(day, day, day, day, day, day, day, day)
    print 'rm -rf /data1/odin/dayily_count/'
    os.system('rm -rf /data1/odin/dayily_count/')
    print "hive -e \"%s\"" % daily_sql
    os.system("hive -e \"%s\"" % daily_sql)
    print 'echo ",nav visit,visit reqid,visit uid,search,search reqid,search uid,ad imp,imp reqid,imp uid" > /data1/odin/odin_count_%s.csv' % day
    os.system('echo ",nav visit,visit reqid,visit uid,search,search reqid,search uid,ad imp,imp reqid,imp uid, ad_click, click_reqid, click_uid" > /data1/odin/odin_count_%s.csv' % day)
    print 'cat /data1/odin/dayily_count/0000* >> /data1/odin/odin_count_%s.csv' % day
    os.system('cat /data1/odin/dayily_count/0000* >> /data1/odin/odin_count_%s.csv' % day)
    print 'send mail /data1/odin/odin_count_%s.csv '%day
    sendMail(day,'',['/data1/odin/odin_count_%s.csv' % day])

if __name__ == '__main__':
    if len(sys.argv) == 2:
        day = sys.argv[1]
    else:
        day = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")

    count_odin(day)

