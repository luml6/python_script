import requests
import json
from _mysql import OperateSQL



def update_package():
    sql="select * from temp_module_20180514 where  id>=23690"
    issuccess,query_data = OperateSQL('package_server').query_get(sql)
    # print (query_data)
    if issuccess:
        for var in query_data:
            types,sub_type,display_name,app_tags = get_mi_app(var['src_package_name'])
            # print(types,sub_type,display_name,app_tags)
            update_sql =  '''
            UPDATE  temp_module_20180514 SET category="'''+types+'''",sub_category="'''+sub_type+'''" ,display_name="'''+display_name+'''" , app_tags="'''+app_tags+'''"  WHERE id = "'''+str(var["id"])+'''" 
            '''
            # print(update_sql)
            update_succ,update_result = OperateSQL("package_server").query_update(update_sql)
            if not update_succ:
                print('update err')



def get_mi_app(package_name):
    types=''
    sub_type=''
    display_name=''
    app_tage=''
    print('search mi package')
    payload = {
        'imei':'867a1dd6efaa473c07d6be394d83f569',
        'clientId':'b961d9c7cdc41f3868176a0f3cc81912',
        'sdk':23,
        'os':'7.11.15',
        'la':'zh',
        'co':'CN',
        'miuiBigVersionName':'V9-dev',
        'model':'MI%204LTE',
        'pageRef':'com.miui.home',
        'ref':'xxx'
            
    }
    ret = requests.get('https://app.market.xiaomi.com/apm/app/package/'+package_name,params = payload)
    resp = json.loads(ret.text)
    # print(resp)
    # if resp['status']=='1':
    #     return resp
    types = resp['app'].get('level1CategoryName','')
    sub_type = resp['app'].get('level2CategoryName','')
    display_name = resp['app'].get('displayName','')
    appTags = resp['app'].get('appTags',None)
    print(appTags)
    if appTags:
        for index,var in enumerate(appTags):
            app_tage+=var.get('tagName')
            if index!=len(appTags)-1:
                app_tage+=','
    return types,sub_type,display_name,app_tage


if __name__=='__main__':
    update_package()