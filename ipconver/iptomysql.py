#!/usr/bin/env python
# coding=utf-8
# kbdancer@92ez.com
 
import sys
from _mysql import OperateSQL
from test import ip2int
from test import split_area

# reload(sys)
# sys.setdefaultencoding('utf8')
 
 
def save_data_to_mysql(ip_line):
    try:
        begin = ip_line[0:16].replace(' ', '')
        end = ip_line[16:32].replace(' ', '')
        try:
            location = line[32:].split(' ')[0]
        except:
            location = ''
        try:
            isp_type = line[32:].replace(' ', ' ').split(' ')[1].replace('\n', '').replace('\r', '')
        except:
            isp_type = ''
        startIp = ip2int(begin)
        endIp = ip2int(end)
        province,city,district = split_area(location)
        this_line_value = [begin, end, location, isp_type,startIp,endIp,province,city,district]
        print(this_line_value)
        do_insert(this_line_value)
    except Exception as e:
        print (e)
 
 
def do_insert(row_data):
    try:
        insert_sql = """INSERT INTO `ip_addr_copy_new` (`start_ip`,`end_ip`, `area`,`remark`,`start_num`,`end_num`,`province`,`city`,`district`) VALUES ( %s, %s, %s,%s, %s, %s,%s, %s, %s )"""
  
        get_succ,get_result=OperateSQL("device_server").insert(insert_sql,row_data)
    except Exception as e:
        print (row_data)
        print (e)
 


 
if __name__ == '__main__':
    ip_file = open(sys.path[0] + "/test.txt")
    print('Start save to mysql ...')
    for line in ip_file:
        save_data_to_mysql(line)
    ip_file.close()
    print('Save complete.')