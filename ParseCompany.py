# encoding: utf-8
__author__ = 'Defei'
import json
import MySQLdb
import sys
reload(sys)
sys.setdefaultencoding('UTF-8')
from datetime import datetime

company_detail = "http://app.qichacha.com//enterprises/new/newGetData?province=s&user=&unique="

conn = MySQLdb.connect(host="192.168.0.107", user="root", passwd="root123", db="haolaoban", charset="utf8")
cursor = conn.cursor()

def insert_company_info(company_data):

    name = company_data['Name']
    source_url = company_detail + company_data['Unique']
    registered_captial = company_data['RegistCapi']
    oper_name = company_data['OperName']
    founded_date = company_data['StartDate'] if company_data['StartDate'] else '0000-00-00T00:00:00+8.0'
    company_type = company_data['EconKind']
    status_of_operation = company_data['Status']
    business_scope = company_data['Scope']
    registered_addr = company_data['Address']
    term_start = company_data['TermStart'] if company_data['TermStart'] else '0000-00-00T00:00:00+8.0'
    term_end = company_data['TeamEnd'] if company_data['TeamEnd'] else '0000-00-00T00:00:00+8.0'
    regist_authority = company_data['BelongOrg']
    check_date = company_data['CheckDate'] if company_data['CheckDate'] else '0000-00-00T00:00:00+8.0'
    company_logo = ''
    reg_num = company_data['No']
    county = company_data['Area']['County']

    province = company_data['Area']['Province']
    city = company_data['Area']['City']

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    sql = "insert into company_regist_info(name,source_url,registered_capital, " \
          "oper_name,founded_date,company_type,status_of_operation,business_scope,registered_addr," \
          "term_start,term_end,regist_authority,check_date,company_logo,reg_num, province, " \
          "city, county, created_time,update_time) values(" \
          "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    try:
        cursor.execute(sql, (name, source_url, registered_captial, oper_name, founded_date.strip().split('T')[0],
                                 company_type, status_of_operation, business_scope, registered_addr, term_start.strip().split('T')[0],
                                 term_end.strip().split('T')[0], regist_authority, check_date.strip().split('T')[0], company_logo,
                                 reg_num, province, city, county, now, now))
        insert_id = conn.insert_id()

        return insert_id

    except MySQLdb.MySQLError, e:
        try:
            sqlError = "Error %d:%s" % (e.args[0], e.args[1])
            print sqlError
        except IndexError:
            print "MySQL Error:%s" % str(e)
        return None

def insert_parter_info(company_id, partner_info):
    stock_type = partner_info['StockType']
    stock_name = partner_info['StockName']
    certificate_type = partner_info['IdentifyType']
    certificate_info = partner_info['IdentifyNo']

    subscribe_capital = partner_info['ShouldCapi']
    subs_capi_date = partner_info['ShoudDate'] if partner_info['ShoudDate'] else '1900年01月01日'
    subs_capi_date = datetime.strptime(subs_capi_date.encode('utf-8'), '%Y年%m月%d日').strftime("%Y-%m-%d")

    subs_capi_way = partner_info['InvestType']

    real_capital = partner_info['RealCapi']
    real_capi_date = partner_info['CapiDate'] if partner_info['CapiDate'] else '1900年01月01日'
    real_capi_date = datetime.strptime(real_capi_date.encode('utf-8'), '%Y年%m月%d日').strftime("%Y-%m-%d")
    real_capi_way = partner_info['InvestType']

    stock_percent = partner_info['StockPercent']

    time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    sql = "insert into sotckholder(company_id,stock_type,stock_name, " \
          "certificate_type,certificate_info,subscribe_capital,subs_capi_date,subs_capi_way,real_capital," \
          "real_capi_date,real_capi_way,stock_percent,created_time,update_time) values(" \
          "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    try:
        cursor.execute(sql, (company_id, stock_type, stock_name, certificate_type, certificate_info, subscribe_capital,
                             subs_capi_date, subs_capi_way, real_capital, real_capi_date.strip().split('T')[0], real_capi_way, stock_percent,
                             time, time))
        return conn.insert_id()
    except MySQLdb.MySQLError, error:
        try:
            sql_error = "Error %d:%s" % (error.args[0], error.args[1])
            print sql_error
        except IndexError:
            print "MySQL Error:%s" % str(error)
        return None


def parse_company(json_data):
    try:
        obj = json.loads(json_data)
        company_data = obj['Company']
        new_company_id = insert_company_info(company_data)
        if new_company_id is None:
            conn.rollback()
            with open('error.txt', 'a') as ferr:
                ferr.write(json_data)
            return None
        # 股东结构
        partners = company_data['Partners']
        for val in partners:
            id = insert_parter_info(new_company_id, val)
            if id is None:
                cursor.close()
                conn.rollback()
                conn.close()
                with open('error.txt', 'a') as ferr:
                    ferr.write(json_data)
                return None
        cursor.close()
        conn.commit()
        conn.close()
        print 'success!'
        with open('success.txt', 'a') as fsu:
            fsu.write(json_data)
        return True

    except Exception, e:
        with open('error.txt', 'a') as ferr:
            ferr.write(json_data)
        print 'parse error: '.decode('utf-8'), str(e)





if __name__ == '__main__':
    with open('company.json', 'r') as fr:
        i = 0
        for line in fr:
            try:
                res = parse_company(line)
                print res
            except Exception, e:
                print 'parse error: ', str(e)
