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



# sys.stdout = io.TextIOWrapper(sys.stdout.buffer,encoding='gb18030') #改变标准输出的默认编码
sys.setdefaultencoding('utf8')
gaode_web_key = '3f94f559f95f59d85462cd6996b1c78e'
gaode_cell_key = 'fa62ce145d7b0333aa8c8ee42008fd6b'
 
def data_controller():
    get_succ,get_result=OperateSQL("bs_rom_data_server").query_get('select * from device_active_data  where id in ( select  max(id) from device_active_data group by imei0) and location_state=0  limit 1000')
    if not get_succ:
        print("err")
    # print(get_result)
    #打印表中的多少数据
    for var in get_result:
        get_succ = False
        is_gsm = True
        mcc = None
        mnc = None
        cid = None
        lac = None
        imsi = None
        result = None
        province = None
        city = None
        district = None
        area = None
        mcc = var['mcc']
        cid = var['cid']
        mnc = var['mnc']
        lac = var['lac']
        if cid=='-1':
            is_gsm = False
            if len(var['cdmasid'])>0:
                mnc = var['cdmasid']
                lac = var['cdmanid']
                cid = var['cdmabid']
                sql = '''select * from global_cdma where sid="'''+mnc+'''" and nid="'''+lac+'''" and bid="'''+cid+'''" '''
                print(sql)
                get_succ,result=OperateSQL("bs_rom_data_server").query_get(sql)
                if not get_succ:
                    result=None
                    print("select cmda err")  
        else:
            sql = '''select * from global_gsm where mnc="'''+mnc+'''" and lac="'''+lac+'''" and cid="'''+cid+'''" and mcc="'''+mcc+'''" '''
            print(sql)
            get_succ,result=OperateSQL("bs_rom_data_server").query_get(sql)
            # print(get_succ,result)
            if not get_succ:
                result=None
                print("select gsm err")
        
        if  result is None or len(result)==0:
            if var['imsi0']!='0':
                imsi=var['imsi0']
            else:
                imsi = var['imsi1']
            gaode_rep = gaode_cell_get(mcc,mnc,lac,cid,var['imei0'],imsi,is_gsm)
            print(gaode_rep)
            if gaode_rep and gaode_rep['result']['type']!='0' and 'desc' in gaode_rep['result'].keys():
                address = gaode_rep['result']['desc'].replace(' ', '')
                _,_,district = place_cut(address)
                print(address,district)
                province = gaode_rep['result'].get('province','')
                city = gaode_rep['result'].get('city','')
                country = gaode_rep['result'].get('country','')
                update_sql = '''
                UPDATE  device_active_data SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+gaode_rep['result']['desc']+'''" WHERE id = "'''+str(var["id"])+'''" 
                '''
                update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                if update_succ:
                    print(update_succ)
                    insert_jizhan(is_gsm,mnc,mcc,cid,lac,province,city,district,gaode_rep['result']['desc'],country)
                else:
                    update_sql = '''
                    UPDATE  device_active_data SET location_state=2 WHERE id = "'''+str(var["id"])+'''" 
                    '''
                    update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                update_succ = False   
            elif var['gps'] is not None and var['gps']!='':
                gps_rep=gps_cell_get(var['gps'])  
                if gps_rep and not isinstance(gps_rep['regeocode']['formatted_address'], list):
                    address=gps_rep['regeocode']['formatted_address'].replace(' ', '')
                    province,city,district = place_cut(address)
                    country = gps_rep['regeocode']['addressComponent']['country']
                    update_sql = '''
                    UPDATE  device_active_data SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+address+'''" WHERE id = "'''+str(var["id"])+'''" 
                    '''
                    update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                    if not update_succ:
                        print('update by GPS err')
                else:
                    country,area,province,city,district=ip_cell_get(var['ip']) 
                    update_sql = '''
                    UPDATE  device_active_data SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+area+'''" WHERE id = "'''+str(var["id"])+'''" 
                    '''
                    update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                    if not update_succ:
                        print('update by IP err')
            else:
                country,area,province,city,district=ip_cell_get(var['ip']) 
                update_sql = '''
                UPDATE  device_active_data SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+area+'''" WHERE id = "'''+str(var["id"])+'''" 
                '''
                update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                if not update_succ:
                    print('update by IP err')
        else:
            if len(result)>0:
                address = result[0]['area'].replace(' ', '')
                province,city,district = place_cut(address)
                # district = result[0]['district']
                # province = result[0]['province']
                # city = result[0]['city']
                country = result[0]['country']
                update_sql = '''
                UPDATE  device_active_data SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+address+'''" WHERE id = "'''+str(var["id"])+'''" 
                '''
                update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql) 
                        # lbs_rep = lbs_cell_get(mcc,mnc,lac,cid)
                        # print(lbs_rep)
                        # if lbs_rep:
                        #         province,city,district = split_area(lbs_rep['address'])
                        #         update_sql = '''
                        #         UPDATE  device_active_data SET country="中国" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+lbs_rep['address']+'''" WHERE id = "'''+str(var["id"])+'''" 
                        #         '''
                        #         print(update_sql)
                        #         update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                        #         if get_succ:
                        #               insert_jizhan(is_gsm,mnc,mcc,cid,lac,province,city,district,lbs_rep['address'])
                        # else:
                        #         gps_cell_get()   
                        # 
#Use IP Get Cell Address
def ip_cell_get(ip):
    print('use IP get cell address')
    province = ""
    city = ""
    district = ""
    area = ""
    num_ip=ip2int(str(ip))
    print num_ip
    sql='''
    SELECT area,province,city,district FROM global_ip_addr WHERE start_num<="''' + str(num_ip)+'''" AND end_num>="''' + str(num_ip)+'''"
    '''
    search_succ,search_result = OperateSQL("bs_rom_data_server").query_get(sql)
    if not search_succ:
        print('ip search err')
        return "","","","",""
    print(search_result)
    if search_result:
        print search_result
        country = '中国'
        area = search_result[0]["area"].replace(' ','')
        province,city,district = place_cut(area)
        # province = search_result[0]["province"]
        # city = search_result[0]["city"]
        # district = search_result[0]["district"]
        if area == "局域网":
            province = "上海市"
            city = "上海市"
            district = "浦东新区"
        if city is None:
            city=""
        if province is None:
            province = ""
        if district is None:
            district = ""
    print(area,province,city,district)
    
    return country,area,province,city,district       

def ip2int(ip):
    ip_list = ip.strip().split('.')
    SUM = 0
    for i in range(len(ip_list)):
        SUM += int(ip_list[i])*256**(3-i)
    return SUM         
# gaode gps x,y change
def goade_gps_change(gps):
    gps_list = gps.split(',')
    if len(gps_list)>1:
        new_gps = gps_list[1]+','+gps_list[0]
        return new_gps
    return gps
     
# Use GPS Get Cell Address
def gps_cell_get(gps):
        print('use GPS get cell address')
        gps = goade_gps_change(gps)
        print(gps)
        payload = {
            'key':gaode_web_key,
            'location':gps,
            'poitype':"",
            'radius':1000,
            'extensions':'base',
            'batch':False,
            'roadlevel':0
        }
        ret = requests.get('http://restapi.amap.com/v3/geocode/regeo',params = payload)
        # print(ret.url)
        resp = json.loads(ret.text)
        print(resp)
        if resp['status']=='1':
                return resp
        return None

# Use lbs Get Cell Address
def lbs_cell_get(mcc,mnc,lac,ci):
        payload = {
            'mcc':mcc,
            'mnc':mnc,
            'lac':lac,
            'ci':ci,
            'output':'json'
        }
        ret = requests.get('http://api.cellocation.com:81/cell/',params = payload)
        resp = json.loads(ret.text)
        print(resp)
        if resp['errcode']==0:
            return resp
        return None

def gaode_cell_get(mcc,mnc,lac,ci,imei,imsi,is_gsm):
    print('use Jizhan get cell address')
    payload = {
        'imei':imei,
        'imsi':imsi,
        'accesstype':0,
        'output':'json',
        'key':gaode_cell_key,
            
    }
    if is_gsm:
        payload['bts'] = str(mcc)+','+str(mnc)+','+str(lac)+','+str(ci)+',-52'
        payload['cdma'] = 0
    else:
        payload['bts'] = str(mnc)+','+str(lac)+','+str(ci)+',,,-52'
        payload['cdma'] = 1
    ret = requests.get('http://apilocate.amap.com/position',params = payload)
    resp = json.loads(ret.text)
    # print(resp)
    if resp['status']=='1':
        return resp
    return None
        

# insert address to our Mysql
def insert_jizhan(is_gsm,mnc,mcc,cid,lac,province,city,district,area,country):
    if is_gsm:
        insert_sql = '''
        INSERT into global_gsm(mnc,mcc,cid,lac,province,city,district,country,area)VALUES("'''+mnc+'''" ,"'''+mcc+'''" ,"'''+cid+'''" ,"'''+lac+'''","'''+province+'''" ,"'''+city+'''" ,"'''+district+'''","'''+country+'''","'''+area+'''")
        '''
        insert_succ,insert_result = OperateSQL("bs_rom_data_server").query_update(insert_sql)
        if not insert_succ: 
            print('insert gsm err')
    else:
        insert_sql = '''
        INSERT into global_cdma(sid,nid,bid,province,city,district,country,area)VALUES("'''+mnc+'''" ,"'''+cid+'''" ,"'''+lac+'''","'''+province+'''" ,"'''+city+'''" ,"'''+district+'''","'''+country+'''","'''+area+'''")
        '''
        insert_succ,insert_result = OperateSQL("bs_rom_data_server").query_update(insert_sql)
        if not insert_succ: 
            print('insert cmda err')




# split area to province,city,distrcit 
def split_area(area):
        count = 0
        province=None
        city = None
        district = None
        if "省" in area:
            areas=re.split("省|市|州|县|区|市",area)
            province=areas[0]+"省"
            print(province)

            city = ""
            if len(areas)>=2:
                if len(areas[1])>0:
                    if "州" in area:
                        city = areas[1]+"州"
                    else:
                        city = areas[1]+"市"
            print(city)

            district = ""
            if len(areas)>=3:
                if len(areas[2])>0:
                    if "县" in area:
                        district = areas[2]+"县"
                    elif "区" in area:
                        district = areas[2]+"区"
                    else:
                        district = areas[2]+"市"
            print(len(district))
            return province,city,district
        if "北京" in area or "上海" in area or "重庆" in area or "天津" in area:
            count+=1
            areas=re.split("市|区|县|镇",area)
            province=areas[0]+"市"
            print(province)

            city = areas[0]+"市"
            district = ""
            if len(areas)>=2:
                if len(areas[1])>0:
                    if "区" in area:
                        district = areas[1]+"区"
                    if "县" in area:
                        district = areas[1]+"县"
            return province,city,district
        #     district = ""
        #     if len(areas)>=3:
        #         if len(areas[2])>0:
        #             district = areas[2]+"镇"
        #     print len(district)
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
            return province,city,district
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
            return province,city,district
        if "广西" in area:
            count+=1
            areas=re.split("广西|市|县|区|市",area)
            province="广西壮族自治区"
            print(province)

            city = ""
            if len(areas)>=2:
                if len(areas[1])>0:
                    city = areas[1]+"市"
            print(city)

            district = ""
            if len(areas)>=3:
                if len(areas[2])>0:
                    if "县" in area:
                        district = areas[2]+"县"
                    elif "区" in area:
                        district = areas[2]+"区"
                    else:
                        district = areas[2]+"市"
            print(len(district))
            return province,city,district
        if "宁夏" in area:
            count+=1
            areas=re.split("宁夏|市|县|区|市",area)
            province="宁夏回族自治区"
            print(province)
            city = ""
            if len(areas)>=2:
                if len(areas[1])>0:
                    city = areas[1]+"市"
            print(city)

            district = ""
            if len(areas)>=3:
                if len(areas[2])>0:
                    if "县" in area:
                        district = areas[2]+"县"
                    elif "区" in area:
                        district = areas[2]+"区"
                    else:
                        district = areas[2]+"市"
            return province,city,district
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
            return province,city,district
        if "香港特別行政區" in area:   
            areas = re.split("香港特別行政區|州|地区|市|市|县|區",area)
            province = "香港特別行政區"
            city = "香港"
            district = ""
            if len(areas)>=3:
                if len(areas[1])>0:
                    if "县" in area:
                        district = areas[1]+"县"
                    elif "區":
                        district = areas[1]+"區"
            return province,city,district
        if "澳门特別行政區" in area:
             
            areas = re.split("澳门特別行政區|區",area)
            province = "澳门特別行政區"
            city = "澳门"
            district = ""
            if len(areas)>=3:
                if len(areas[1])>0:
                    if "县" in area:
                        district = areas[1]+"县"
                    elif "區":
                        district = areas[1]+"區"
        
        return province,city,district 


def first_data_controller():
    get_succ,get_result=OperateSQL("bs_rom_data_server").query_get('select * from first_active where city not like "%市"  and LENGTH(city)>=4 and LENGTH(city)<=9 and city not like "%州"')
    if not get_succ:
        print("err")
    # print get_result
    #打印表中的多少数据
    for var in get_result:
        get_succ = False
        is_gsm = True
        mcc = None
        mnc = None
        cid = None
        lac = None
        imsi = None
        result = None
        province = None
        city = None
        district = None
        area = None
        mcc = var['mcc']
        cid = var['cid']
        mnc = var['mnc']
        lac = var['lac']
        if cid=='-1':
            is_gsm = False
            if len(var['cdmasid'])>0:
                mnc = var['cdmasid']
                lac = var['cdmanid']
                cid = var['cdmabid']
                sql = 'select * from global_cdma where sid='+mnc+' and nid='+lac+' and bid='+cid+' '
                print(sql)
                get_succ,result=OperateSQL("bs_rom_data_server").query_get(sql)
                if not get_succ:
                    result=None
                    print("select cmda err")  
        else:
            sql = 'select * from global_gsm where mnc='+mnc+' and lac='+lac+' and cid='+cid+' and mcc='+mcc+' '
            print(sql)
            get_succ,result=OperateSQL("bs_rom_data_server").query_get(sql)
            # print(get_succ,result)
            if not get_succ:
                result=None
                print("select gsm err")
        
        if  result is None or len(result)==0:
            if var['imsi0']!='0':
                imsi=var['imsi0']
            else:
                imsi = var['imsi1']
            gaode_rep = gaode_cell_get(mcc,mnc,lac,cid,var['imei0'],imsi,is_gsm)
            print(gaode_rep)
            if gaode_rep and gaode_rep['result']['type']!='0' and 'desc' in gaode_rep['result'].keys():
                address = gaode_rep['result']['desc'].replace(' ', '')
                _,_,district = place_cut(address)
                print(address,district)
                province = gaode_rep['result'].get('province','')
                city = gaode_rep['result'].get('city','')
                country = gaode_rep['result'].get('country','')
                update_sql = '''
                UPDATE  first_active SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+gaode_rep['result']['desc']+'''" WHERE id = "'''+str(var["id"])+'''" 
                '''
                update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                if update_succ:
                    print(update_succ)
                    insert_jizhan(is_gsm,mnc,mcc,cid,lac,province,city,district,gaode_rep['result']['desc'],country)
                else:
                    update_sql = '''
                    UPDATE  first_active SET location_state=2 WHERE id = "'''+str(var["id"])+'''" 
                    '''
                    update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                update_succ = False   
            elif var['gps'] is not None and var['gps']!='':
                gps_rep=gps_cell_get(var['gps'])  
                if gps_rep :
                    
                    address=gps_rep['regeocode']['formatted_address'].replace(' ', '')
                    province,city,district = place_cut(address)
                    country = gps_rep['regeocode']['addressComponent']['country']
                    update_sql = '''
                    UPDATE  first_active SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+address+'''" WHERE id = "'''+str(var["id"])+'''" 
                    '''
                    update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                    if not update_succ:
                        print('update by GPS err')
                    else:
                        country,area,province,city,district=ip_cell_get(var['ip']) 
                        update_sql = '''
                        UPDATE  first_active SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+area+'''" WHERE id = "'''+str(var["id"])+'''" 
                        '''
                        update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                        if not update_succ:
                            print('update by IP err')
                else:
                    country,area,province,city,district=ip_cell_get(var['ip']) 
                    update_sql = '''
                    UPDATE  first_active SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+area+'''" WHERE id = "'''+str(var["id"])+'''" 
                    '''
                    update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                    if not update_succ:
                        print('update by IP err')
            else:
                country,area,province,city,district=ip_cell_get(var['ip']) 
                update_sql = '''
                UPDATE  first_active SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+area+'''" WHERE id = "'''+str(var["id"])+'''" 
                '''
                update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                if not update_succ:
                    print('update by IP err')
        else:
            if len(result)>0:
                address = result[0]['area'].replace(' ', '')
                province,city,district = place_cut(address)
                # district = result[0]['district']
                # province = result[0]['province']
                # city = result[0]['city']
                country = result[0]['country']
                update_sql = '''
                UPDATE  first_active SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+address+'''" WHERE id = "'''+str(var["id"])+'''" 
                '''
                update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql) 



def handle_first_data_controller():
    get_succ,get_result=OperateSQL("bs_rom_data_server").query_get('select * from handle_first_active where  city not like "%市"  and LENGTH(city)>=4 and LENGTH(city)<=9 and city not like "%州" ')
    if not get_succ:
        print("err")
    # print get_result
    #打印表中的多少数据
    for var in get_result:
        get_succ = False
        is_gsm = True
        mcc = None
        mnc = None
        cid = None
        lac = None
        imsi = None
        result = None
        province = None
        city = None
        district = None
        area = None
        mcc = var['mcc']
        cid = var['cid']
        mnc = var['mnc']
        lac = var['lac']
        if cid=='-1':
            is_gsm = False
            if len(var['cdmasid'])>0:
                mnc = var['cdmasid']
                lac = var['cdmanid']
                cid = var['cdmabid']
                sql = 'select * from global_cdma where sid='+mnc+' and nid='+lac+' and bid='+cid+' '
                print(sql)
                get_succ,result=OperateSQL("bs_rom_data_server").query_get(sql)
                if not get_succ:
                    result=None
                    print("select cmda err")  
        else:
            sql = 'select * from global_gsm where mnc='+mnc+' and lac='+lac+' and cid='+cid+' and mcc='+mcc+' '
            print(sql)
            get_succ,result=OperateSQL("bs_rom_data_server").query_get(sql)
            # print(get_succ,result)
            if not get_succ:
                result=None
                print("select gsm err")
        
        if  result is None or len(result)==0:
            if var['imsi0']!='0':
                imsi=var['imsi0']
            else:
                imsi = var['imsi1']
            gaode_rep = gaode_cell_get(mcc,mnc,lac,cid,var['imei0'],imsi,is_gsm)
            print(gaode_rep)
            if gaode_rep and gaode_rep['result']['type']!='0' and 'desc' in gaode_rep['result'].keys():
                address = gaode_rep['result']['desc'].replace(' ', '')
                _,_,district = place_cut(address)
                print(address,district)
                province = gaode_rep['result'].get('province','')
                city = gaode_rep['result'].get('city','')
                country = gaode_rep['result'].get('country','')
                update_sql = '''
                UPDATE  handle_first_active SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+gaode_rep['result']['desc']+'''" WHERE id = "'''+str(var["id"])+'''" 
                '''
                update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                if update_succ:
                    print(update_succ)
                    insert_jizhan(is_gsm,mnc,mcc,cid,lac,province,city,district,gaode_rep['result']['desc'],country)
                else:
                    update_sql = '''
                    UPDATE  handle_first_active SET location_state=2 WHERE id = "'''+str(var["id"])+'''" 
                    '''
                    update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                update_succ = False   
            elif var['gps'] is not None and var['gps']!='':
                gps_rep=gps_cell_get(var['gps'])  
                if gps_rep:
                    address=gps_rep['regeocode']['formatted_address'].replace(' ', '')
                    province,city,district = place_cut(address)
                    country = gps_rep['regeocode']['addressComponent']['country']
                    update_sql = '''
                    UPDATE  handle_first_active SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+address+'''" WHERE id = "'''+str(var["id"])+'''" 
                    '''
                    update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                    if not update_succ:
                        print('update by GPS err')
                else:
                    country,area,province,city,district=ip_cell_get(var['ip']) 
                    update_sql = '''
                    UPDATE  handle_first_active SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+area+'''" WHERE id = "'''+str(var["id"])+'''" 
                    '''
                    update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                    if not update_succ:
                        print('update by IP err')
            else:
                country,area,province,city,district=ip_cell_get(var['ip']) 
                update_sql = '''
                UPDATE  handle_first_active SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+area+'''" WHERE id = "'''+str(var["id"])+'''" 
                '''
                update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
                if not update_succ:
                    print('update by IP err')
        else:
            if len(result)>0:
                address = result[0]['area'].replace(' ', '')
                province,city,district = place_cut(address)
                print "test"
                print city
                # district = result[0]['district']
                # province = result[0]['province']
                # city = result[0]['city']
                country = result[0]['country']
                update_sql = '''
                UPDATE  handle_first_active SET location_state=1,country="'''+country+'''" ,province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''",area="'''+address+'''" WHERE id = "'''+str(var["id"])+'''" 
                '''
                update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql) 

def change_ip_addr():
    get_succ,get_result=OperateSQL("bs_rom_data_server").query_get('select * from global_ip_addr where  province="贵省"')
    if not get_succ:
        print("err")
    # print get_result
    #打印表中的多少数据
    for var in get_result:
        address =var['area'].replace(' ', '')
        province,city,district = place_cut(address)
        update_sql = '''
        UPDATE  global_ip_addr SET province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''" WHERE start_ip = "'''+str(var["start_ip"])+'''" 
        '''
        update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql) 

def change_cid_addr():
    get_succ,get_result=OperateSQL("bs_rom_data_server").query_get('SELECT * from global_cdma where city like "%省" ')
    if not get_succ:
        print("err")
    # print get_result
    #打印表中的多少数据
    for var in get_result:
        address =var['area'].replace(' ', '')
        province,city,district = place_cut(address)
        update_sql = '''
        UPDATE  global_cdma SET province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''" WHERE sid = "'''+str(var["sid"])+'''" and nid = "'''+str(var["nid"])+'''" and bid = "'''+str(var["bid"])+'''" 
        '''
        update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql) 
def change_cdma_addr():
    get_succ,get_result=OperateSQL("bs_rom_data_server").query_get('SELECT * from global_gsm where city like "%省" ')
    if not get_succ:
        print("err")
    # print get_result
    #打印表中的多少数据
    for var in get_result:
        address =var['area'].replace(' ', '')
        province,city,district = place_cut(address)
        update_sql = '''
        UPDATE  global_gsm SET province="'''+province+'''" , city="'''+city+'''" , district="'''+district+'''" WHERE cid = "'''+str(var["cid"])+'''" and lac = "'''+str(var["lac"])+'''" and mcc = "'''+str(var["mcc"])+'''" and mnc = "'''+str(var["mnc"])+'''" 
        '''
        update_succ,update_result = OperateSQL("bs_rom_data_server").query_update(update_sql)
        
if __name__=='__main__':

    #BlockingScheduler
    # scheduler = BlockingScheduler()
    # scheduler.add_job(data_controller, 'cron', day_of_week='1-6', hour=6, minute=30)
    # scheduler.add_job(first_data_controller, 'cron', day_of_week='1-6', hour=6, minute=30)
    # scheduler.add_job(handle_first_data_controller, 'cron', day_of_week='1-6', hour=6, minute=30)
    # scheduler.start()
    # scheduler = BlockingScheduler()
    # scheduler.add_job(data_controller, 'interval', minutes=5)
    # scheduler.add_job(first_data_controller, 'interval', minutes=5)
    # scheduler.add_job(handle_first_data_controller, 'interval', minutes=5)
    # scheduler.add_job(change_ip_addr, 'interval', minutes=5)
    # scheduler.start()
    count=0
    while (count < 100):
        data_controller()
        count = count+1
    #first_data_controller()
    #handle_first_data_controller()
    #change_cid_addr()
    #change_cdma_addr()
    # change_ip_addr()