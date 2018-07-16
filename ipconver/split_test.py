#coding=utf-8
import re
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def place_cut(data):
    print data
    province = ''
    city = ''
    district = ''
    if "内蒙古" in data:
        areas = re.split("内蒙古自治区|内蒙古|盟|市|旗|市|区|县",data)
        province = "内蒙古自治区"
        city = ""
        if len(areas)>=2:
            if len(areas[1])>0:
                if "市" in data:
                    city = areas[1]+"市"
                if "盟" in data:
                    city = areas[1]+"盟"
        district = ""
        if len(areas)>=3:
            if len(areas[2])>0:
                if "县" in data:
                    district = areas[2]+"县"
                elif "区" in data:
                    district = areas[2]+"区"
                elif "市" in data:
                    district = areas[2]+"市"
                else:
                    district = areas[2]+"旗"
        return province,city,district
    if "新疆" in data:
        areas = re.split("新疆维吾尔自治区|新疆|州|地区|市|区|市|县",data)
        province = "新疆维吾尔自治区"
        city = ""
        if len(areas)>=2:
            if len(areas[1])>0:
                if "州" in data:
                    city = areas[1]+"州"
                elif "地区" in data:
                    city = areas[1]+"地区"
                else:
                    city = areas[1]+"市"
        district = ""
        if len(areas)>=3:
            if len(areas[2])>0:
                if "县" in data:
                    district = areas[2]+"县"
                elif "区" in data:
                    district = areas[2]+"区"
                else:
                    district = areas[2]+"市"
        return province,city,district
    if "广西" in data:
        areas=re.split("广西壮族自治区|广西|市|县|区|市",data)
        province="广西壮族自治区"
        city = ""
        if len(areas)>=2:
            if len(areas[1])>0:
                city = areas[1]+"市"
        print(city)
        district = ""
        if len(areas)>=3:
            if len(areas[2])>0:
                if "县" in data:
                    district = areas[2]+"县"
                elif "区" in data:
                    district = areas[2]+"区"
                else:
                    district = areas[2]+"市"
        return province,city,district
    if "宁夏" in data:
        areas=re.split("宁夏回族自治区|宁夏|市|县|区|市",data)
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
                if "县" in data:
                    district = areas[2]+"县"
                elif "区" in data:
                    district = areas[2]+"区"
                else:
                    district = areas[2]+"市"
        return province,city,district
    if "西藏" in data:
        areas = re.split("西藏自治区|西藏|州|地区|市|市|县",data)
        province = "西藏自治区"
        city = ""
        if len(areas)>=2:
            if len(areas[1])>0:
                if "州" in data:
                    city = areas[1]+"州"
                elif "地区" in data:
                    city = areas[1]+"地区"
                else:
                    city = areas[1]+"市"
        district = ""
        if len(areas)>=3:
            if len(areas[2])>0:
                if "县" in data:
                    district = areas[2]+"县"
                else:
                    district = areas[2]+"市"
        return province,city,district
    if "香港特別行政區" in data or "香港" in data:   
        areas = re.split("香港特別行政區|州|地区|市|市|县|區",data)
        province = "香港特別行政區"
        city = "香港"
        district = ""
        if len(areas)>=3:
            if len(areas[1])>0:
                if "县" in data:
                    district = areas[1]+"县"
                elif "區":
                    district = areas[1]+"區"
        return province,city,district
    if "澳门特別行政區" in data or "澳门" in data or "澳門特別行政區" in data:   
        areas = re.split("澳门特別行政區|澳門特別行政區|區",data)
        province = "澳門特別行政區"
        city = "澳門"
        district = ""
        if len(areas)>=3:
            if len(areas[1])>0:
                if "县" in data:
                    district = areas[1]+"县"
                elif "區":
                    district = areas[1]+"區"
        return province,city,district
    PATTERN = ur'[中]{0,1}[国]{0,1}([\u4e00-\u9fa5]*?(?:省|自治区|市|新疆|广西|内蒙古|宁夏))([\u4e00-\u9fa5]*?(?:市|区|县|自治州|盟)){0,1}([\u4e00-\u9fa5]*?(?:市|区|县|旗)){0,1}'
    data_utf8 = data.decode('utf8')
    # print data_utf8
    
    pattern = re.compile(PATTERN)
    m = pattern.search(data_utf8)
    if m:
        # 地区信息分为三级
        if m.lastindex >= 1:
            province = m.group(1)
        if m.lastindex >= 2:
            city = m.group(2)
        if m.lastindex >= 3:
            district = m.group(3)
        if city is None:
            city=''
        out = '%s|%s|%s' % (province, city, district)
        print out
    if "北京" in data or "上海" in data or "重庆" in data or "天津" in data:
        district = city
        city = province
    if province=='':
        province='其他'
    return (province, city, district)


if __name__=='__main__':
    province,city,district=place_cut('台湾省靠近床的世界内湖旗艦館')
    out = '%s|%s|%s' % (province.decode('utf8'), city.decode('utf8'), district.decode('utf8'))
    print out
