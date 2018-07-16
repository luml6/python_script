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
from split_test import place_cut
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime

sys.setdefaultencoding('utf8')

def change_address():
    get_succ,get_result=OperateSQL("bs_rom_data_server").query_get('select * from first_active  where  province="宁夏"  limit 100')
    if not get_succ:
        print("err")
    print(get_result)
    #打印表中的多少数据
    for var in get_result:
        province = ""
        city = ""
        district=""
        address = var.get('area','')
        print address
        province,city,district = place_cut(str(address))
        out = '%s|%s|%s' % (province.decode('utf8'), city.decode('utf8'), district.decode('utf8'))
        print out
        # print province,city,district
        update_sql = '''
        UPDATE  first_active SET location_state=1,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''" WHERE id = "'''+str(var["id"])+'''" 
        '''
        update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)

if __name__=='__main__':
    
    change_address()