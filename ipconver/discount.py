#coding=utf-8
# import MySQLdb
import requests
import json
from _mysql import OperateSQL
import sys
import io
import re
import struct
import socket
from datetime import datetime,timedelta
from apscheduler.schedulers.blocking import BlockingScheduler

# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='gb18030') #改变标准输出的默认编码
reload(sys)
sys.setdefaultencoding('utf-8')

def statistics():
    sql = "select * from tbl_online_info where (province is null  or province = '' or province= '错误')  limit 100;"
    while True:
        result, querydata = OperateSQL("device_server").query_get(sql)
        print sql 
        #print result, querydata
        if result:
            for one_item in querydata:                  
                province = GetAreaByIP(one_item["ipaddr"])

                update_sql = "update tbl_online_info set province = '"+province +"' where id = " + str(one_item["id"])
                succe,update_result = OperateSQL("device_server").query_update(update_sql)
                print succe
                print update_sql
            
            if len(querydata) == 0:
                executeActStat()
                break

def executeActStat():
    del_sql = "DELETE FROM device_act_stat_week"
    get_succ,get_result=OperateSQL("device_server").query_update(del_sql)
    del_sql = "DELETE FROM device_act_stat_month"
    get_succ,get_result=OperateSQL("device_server").query_update(del_sql)
    try:
        insert_sql_week = "insert into device_act_stat_week select stat_date,stat_key, province,count(*) as act_num from (select date_sub(create_date,INTERVAL WEEKDAY(create_date)  DAY) stat_date ,model as stat_key, province, udid  from tbl_online_info group by stat_date, province,udid) as temp_a group by stat_key, stat_date,province;"
        get_succ,get_result=OperateSQL("device_server").query_update(insert_sql_week)
        insert_sql_month = "insert into device_act_stat_month select stat_date,stat_key, province,count(*) as act_num from (select concat(date_format(LAST_DAY(create_date),'%Y-%m-'),'01') stat_date , model as stat_key, province, udid  from tbl_online_info group by stat_date, province,udid) as temp_a group by stat_key, stat_date,province;"
        get_succ,get_result=OperateSQL("device_server").query_update(insert_sql_month)

            
    except Exception , e:
        print e
  

def GetAreaByIP(ip_addr):
    try:
        ip_num = getipnum(ip_addr)
        if ip_num == 0:
            return "错误" 
        sql = " select * from global_ipaddr where  start_num < "+str(ip_num)+" and end_num > "+ str(ip_num) + ";"
        
        result, querydata = OperateSQL("device_server").query_get(sql)
        #print querydata
        
        if result:
            for one_item in querydata:
                if not one_item["province"] :
                    #print ip_num
                    return "其他"
                
                if len(one_item["province"]) == 0 :
                    return  "其他"
                return one_item["province"]
        return  "其他"
    except Exception , e:
        print e
        return  "错误"

def getipnum(ipaddr):
    try:
        return socket.ntohl(struct.unpack("I",socket.inet_aton(str(ipaddr)))[0])
    except:
        return 0


if __name__ == '__main__':
    # statistics()
    scheduler = BlockingScheduler()
    scheduler.add_job(statistics, 'cron', day_of_week='1-6', hour=6, minute=30)
    scheduler.start()   
    """
    task.generateTask()
    """