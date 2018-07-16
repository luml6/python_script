# coding=utf-8
from _mysql import OperateSQL
import re


def process_main():
    sql_cmd='''
    SELECT * 
    FROM global_ipaddr_copy 
    WHERE start_ip="27.98.248.0" 
    '''
    get_succ,get_result=OperateSQL("device_server").query_get(sql_cmd)
    if not get_succ:
        print "err"
    count = 0
    for onedata in get_result:
        print onedata["area"]
        area = onedata["area"]
        if "省" in area:
            count+=1
            areas=re.split("省|州|市|县|区|市",area)
            province=areas[0]+"省"
            print province

            city = ""
            if len(areas)>=2:
                if len(areas[1])>0:
                    if "州" in area:
                        city = areas[1]+"州"
                    else:
                        city = areas[1]+"市"
            print city

            district = ""
            if len(areas)>=3:
                if len(areas[2])>0:
                    if "县" in area:
                        district = areas[2]+"县"
                    elif "区" in area:
                        district = areas[2]+"区"
                    else:
                        district = areas[2]+"市"
            print len(district)
            update_sql = '''
            UPDATE  global_ipaddr_copy SET province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''" WHERE start_ip = "'''+str(onedata["start_ip"])+'''" 
            '''
            print update_sql
            update_succ,update_result = OperateSQL("device_server").query_update(update_sql)
        if "北京" in area or "上海" in area or "重庆" in area or "天津" in area:
            count+=1
            areas=re.split("市|区|县|镇",area)
            province=areas[0]+"市"
            print province

            city = ""
            if len(areas)>=2:
                if len(areas[1])>0:
                    if "区" in area:
                        city = areas[1]+"区"
                    if "县" in area:
                        city = areas[1]+"县"
            print city

            district = ""
            if len(areas)>=3:
                if len(areas[2])>0:
                    district = areas[2]+"镇"
            print len(district)
            update_sql = '''
            UPDATE  global_ipaddr_copy SET province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''" WHERE start_ip = "'''+str(onedata["start_ip"])+'''" 
            '''
            print update_sql
            update_succ,update_result = OperateSQL("device_server").query_update(update_sql)
        if "内蒙古" in area:
            count+=1
            areas = re.split("内蒙古|盟|市|旗|市|区|县",area)
            province = "内蒙古自治区"
            city = ""
            if len(areas)>=2:
                if len(areas[1])>0:
                    if "市" in area:
                        city = areas[1]+"市"
                    if "盟" in area:
                        city = areas[1]+"盟"
            district = ""
            if len(areas)>=3:
                if len(areas[2])>0:
                    if "县" in area:
                        district = areas[2]+"县"
                    elif "区" in area:
                        district = areas[2]+"区"
                    elif "市" in area:
                        district = areas[2]+"市"
                    else:
                        district = areas[2]+"旗"
            update_sql = '''
            UPDATE  global_ipaddr_copy SET province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''" WHERE start_ip = "'''+str(onedata["start_ip"])+'''" 
            '''
            print update_sql
            update_succ,update_result = OperateSQL("device_server").query_update(update_sql)
        if "新疆" in area:
            count+=1
            areas = re.split("新疆|州|地区|市|市|县",area)
            province = "新疆维吾尔自治区"
            city = ""
            if len(areas)>=2:
                if len(areas[1])>0:
                    if "州" in area:
                        city = areas[1]+"州"
                    elif "地区" in area:
                        city = areas[1]+"地区"
                    else:
                        city = areas[1]+"市"
            district = ""
            if len(areas)>=3:
                if len(areas[2])>0:
                    if "县" in area:
                        district = areas[2]+"县"
                    else:
                        district = areas[2]+"市"
            update_sql = '''
            UPDATE  global_ipaddr_copy SET province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''" WHERE start_ip = "'''+str(onedata["start_ip"])+'''" 
            '''
            print update_sql
            update_succ,update_result = OperateSQL("device_server").query_update(update_sql)
        if "广西" in area:
            count+=1
            areas=re.split("广西|市|县|区|市",area)
            province="广西壮族自治区"
            print province

            city = ""
            if len(areas)>=2:
                if len(areas[1])>0:
                    city = areas[1]+"市"
            print city

            district = ""
            if len(areas)>=3:
                if len(areas[2])>0:
                    if "县" in area:
                        district = areas[2]+"县"
                    elif "区" in area:
                        district = areas[2]+"区"
                    else:
                        district = areas[2]+"市"
            print len(district)
            update_sql = '''
            UPDATE  global_ipaddr_copy SET province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''" WHERE start_ip = "'''+str(onedata["start_ip"])+'''" 
            '''
            print update_sql
            update_succ,update_result = OperateSQL("device_server").query_update(update_sql)
        if "宁夏" in area:
            count+=1
            areas=re.split("宁夏|市|县|区|市",area)
            province="宁夏回族自治区"
            print province

            city = ""
            if len(areas)>=2:
                if len(areas[1])>0:
                    city = areas[1]+"市"
            print city

            district = ""
            if len(areas)>=3:
                if len(areas[2])>0:
                    if "县" in area:
                        district = areas[2]+"县"
                    elif "区" in area:
                        district = areas[2]+"区"
                    else:
                        district = areas[2]+"市"
            print len(district)
            update_sql = '''
            UPDATE  global_ipaddr_copy SET province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''" WHERE start_ip = "'''+str(onedata["start_ip"])+'''" 
            '''
            print update_sql
            update_succ,update_result = OperateSQL("device_server").query_update(update_sql)

        if "西藏" in area:
            count+=1
            areas = re.split("西藏|州|地区|市|市|县",area)
            province = "西藏自治区"
            city = ""
            if len(areas)>=2:
                if len(areas[1])>0:
                    if "州" in area:
                        city = areas[1]+"州"
                    elif "地区" in area:
                        city = areas[1]+"地区"
                    else:
                        city = areas[1]+"市"
            district = ""
            if len(areas)>=3:
                if len(areas[2])>0:
                    if "县" in area:
                        district = areas[2]+"县"
                    else:
                        district = areas[2]+"市"
            update_sql = '''
            UPDATE  global_ipaddr_copy SET province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''" WHERE start_ip = "'''+str(onedata["start_ip"])+'''" 
            '''
            print update_sql
            update_succ,update_result = OperateSQL("device_server").query_update(update_sql)

    print "count:",count
    print "OVER"


if __name__=='__main__':
    process_main()
