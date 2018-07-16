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
    sql = "select * from tbl_online_info where (province is null  or province = '' ) and create_date>='2018-05-07' limit 10000 ;"
    while True:
        result, querydata = OperateSQL("bs_rom_data_server").query_get(sql)
        print sql 
        #print result, querydata
        if result:
            for one_item in querydata:
                # channel = ''
                # spec=''                  
                province = GetAreaByIP(one_item["ipaddr"])
                # imei = GetImeiByUdid(one_item["udid"])
                # channel,spec=getchannelByiemi(one_item["imei"])
                update_sql = "update tbl_online_info set province='"+province+"' where id = " + str(one_item["id"])
                # update_sql = "update tbl_online_info set imei= '"+one_item["imei"]+"',channel='"+channel+"',spec='"+spec+"',province='"+province+"' where id = " + str(one_item["id"])
                succe,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                # print succe
                # print update_sql
            
            if len(querydata) == 0:
                # executeActStat()
                break

def executeActStat():
    del_sql = "DELETE FROM device_act_stat_week"
    get_succ,get_result=OperateSQL("bs_rom_data_server").query_update(del_sql)
    del_sql = "DELETE FROM device_act_stat_month"
    get_succ,get_result=OperateSQL("bs_rom_data_server").query_update(del_sql)
    try:
        insert_sql_week = "insert into device_act_stat_week select stat_date,stat_key, province,count(*) as act_num from (select date_sub(create_date,INTERVAL WEEKDAY(create_date)  DAY) stat_date ,model as stat_key, province, udid  from tbl_online_info group by stat_date, province,udid) as temp_a group by stat_key, stat_date,province;"
        get_succ,get_result=OperateSQL("bs_rom_data_server").query_update(insert_sql_week)
        insert_sql_month = "insert into device_act_stat_month select stat_date,stat_key, province,count(*) as act_num from (select concat(date_format(LAST_DAY(create_date),'%Y-%m-'),'01') stat_date , model as stat_key, province, udid  from tbl_online_info group by stat_date, province,udid) as temp_a group by stat_key, stat_date,province;"
        get_succ,get_result=OperateSQL("bs_rom_data_server").query_update(insert_sql_month)

            
    except Exception , e:
        print e
  
# def GetImeiByUdid(udid):
#     try:
#         sql = " select * from device_udid_imei where  udid = '"+str(udid)+ "';"
#         print sql
#         result, querydata = OperateSQL("bs_rom_data_server").query_get(sql)
#         print querydata
        
#         if result and len(querydata)>0:
#             for one_item in querydata:
#                 if not one_item["imei"] :
#                     #print ip_num
#                     return ""
                
#                 if len(one_item["imei"]) == 0 :
#                     return  ""
#                 return one_item["imei"]
#         else:
#             sql = " select udid , imei  from device_account  where  udid = '"+str(udid)+ "' limit 1;"
        
#             result, querydata = OperateSQL("online_server").query_get(sql)
#             print querydata
#             if result:
#                 for one_item in querydata:
#                     if not one_item["imei"] :
#                         #print ip_num
#                         return ""
                    
#                     if len(one_item["imei"]) == 0 :
#                         return  ""
#                     insert_sql = '''
#                     INSERT into device_udid_imei(udid,imei)VALUES("'''+str(udid)+'''" ,"'''+one_item["imei"]+'''")
#                     '''
#                     insert_succ,insert_result = OperateSQL("bs_rom_data_server").query_update(insert_sql)
#                     if not insert_succ: 
#                         print('insert gsm err')
#                     return one_item["imei"]
#         return  ""
#     except Exception , e:
#         print e
#         return  ""

def getchannelByiemi(imei):
    
    sql = "select imei0,channel,spec,model from first_active where imei0 = '"+imei+"';"
    print sql
    result, querydata = OperateSQL("bs_rom_data_server").query_get(sql)
    if result and len(querydata)>0:
        for one_item in querydata:
            # spec ='{0}+{1} {2}'.format(one_item["memory"],one_item["storage"],one_item["color"])
            # channel=one_item.get("customer_type","")
            return one_item.get('channel'),one_item.get("spec")
    return '',''
def GetAreaByIP(ip_addr):
    try:
        ip_num = getipnum(ip_addr)
        if ip_num == 0:
            return "错误" 
        sql = " select * from global_ip_addr where  start_num <= "+str(ip_num)+" and end_num => "+ str(ip_num) + ";"
        
        result, querydata = OperateSQL("bs_rom_data_server").query_get(sql)
        #print querydata
        
        if result :
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
    statistics()
    # scheduler = BlockingScheduler()
    # scheduler.add_job(statistics, 'cron', day_of_week='1-6', hour=6, minute=30)
    # scheduler.start()   
    """
    task.generateTask()
    """