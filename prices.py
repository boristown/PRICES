# -*- coding:utf-8 -*-

import mysql.connector
import mypsw
import os
import re
import requests
import datetime
import time


while True:
    print(datetime.datetime.now())
    time.sleep(7)
    try:
        mydb = mysql.connector.connect(
            host=mypsw.host, 
            user=mypsw.user, 
            passwd=mypsw.passwd, 
            database=mypsw.database, 
            auth_plugin='mysql_native_password')
        mycursor = mydb.cursor()
    except Exception as e:
        print(datetime.datetime.now(), ": Connection :" ,e)
        continue

    select_alias_statment = "SELECT DISTINCT symbol, MARKET_TYPE FROM symbol_alias  " \
    " where MARKET_ORDER > 0 " \
    " order by RAND()"

    mycursor.execute(select_alias_statment)

    try:
        alias_results = mycursor.fetchall()
    except Exception as e:
        print(datetime.datetime.now(), ": Fetching :" ,e)
        continue
    if len(alias_results) == 0:
        print(datetime.datetime.now(), "Please maintain market table symbol_alias.")
        continue
    
    startdays = 200
    inputdays = 120

    #url = "https://cn.investing.com/instruments/HistoricalDataAjax"
    url = "https://www.investing.com/instruments/HistoricalDataAjax"

    headers = {
        'accept': "text/plain, */*; q=0.01",
        'origin': "https://www.investing.com",
        'x-requested-with': "XMLHttpRequest",
        'user-agent': "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36",
        'content-type': "application/x-www-form-urlencoded",
        'cache-control': "no-cache",
        'postman-token': "17db1643-3ef6-fa9e-157b-9d5058f391e4"
        }
    
    dateformat = "%m-%d-%Y"

    st_date_str = (datetime.datetime.utcnow() + datetime.timedelta(days = -startdays)).strftime(dateformat).replace("-","%2F")
    end_date_str = (datetime.datetime.utcnow()).strftime(dateformat).replace("-","%2F")
    
    symbol_index = 0
    time_text =  datetime.datetime.utcnow().strftime("%Y%m%d")

    symbol_id_list = []
    
    current_index = 0

    for alias_result in alias_results:
        time.sleep(0.5)
        if alias_result[1] == u'外汇':
            smlID_str = '1072600' #str(int(alias_result[0]) + 106681)
        else:
            smlID_str = '25609849'
        if alias_result[1] == u'加密货币':
            startdays = 130
        else:
            startdays = 200
        st_date_str = (datetime.datetime.utcnow() + datetime.timedelta(days = -startdays)).strftime(dateformat).replace("-","%2F")
        payload = "action=historical_data&curr_id="+ alias_result[0] +"&end_date=" + end_date_str + "&header=null&interval_sec=Daily&smlID=" + smlID_str + "&sort_col=date&sort_ord=DESC&st_date=" + st_date_str
        
        #if alias_result[1] == '外汇':
        print(str(current_index) + '/' + str(len(alias_results)) + ' ' + alias_result[1] + '/' + str(startdays))
        #  print(payload)

        response = None

        try:
            #time.sleep(0.3)
            response = requests.request("POST", url, data=payload, headers=headers, verify=False, timeout=40)
            #break
        except Exception as e:
            print(datetime.datetime.now(), ": request :" ,e)
            time.sleep(7)
            break
        if response == None:
            continue
        table_pattern = r'<tr>.+?<td.+?data-real-value="([^><"]+?)".+?</td>' \
            '.+?data-real-value="([^><"]+?)".+?</td>.+?data-real-value="([^><"]+?)".+?</td>'  \
            '.+?data-real-value="([^><"]+?)".+?</td>.+?data-real-value="([^><"]+?)".+?</td>'  \
            '.+?</tr>'
        row_matchs = re.finditer(table_pattern,response.text,re.S)
        price_list = []
        price_count = 0
        insert_val = []
        #print(str(response.text))
        for cell_matchs in row_matchs:
            price_count += 1
            if price_count > inputdays:
                break
            #print(str(cell_matchs.group(0)))
            price = float(str(cell_matchs.group(2)).replace(",",""))
            #if price_count == 1 or price != price_list[price_count-2]:
            price_list.append(price)
            #else:
            #    price_count -= 1
        if len(price_list) != inputdays:
            #if alias_result[1] == '外汇':
            #  print(len(price_list))
            print(datetime.datetime.now(), ": price_list :" ,len(price_list))
            continue

        insert_val.append((alias_result[0], datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")) + tuple(price_list))

        #insert_val.reverse()
        insert_sql = "INSERT INTO price ("  \
            "SYMBOL, TIME, " + ",".join(["PRICE" + str(i+1).zfill(3) for i in range(inputdays)]) + ") VALUES (" \
            "%s, %s," + ",".join(["%s" for i in range(inputdays)]) + ")"  \
            "ON DUPLICATE KEY UPDATE TIME=VALUES(TIME)," + ",".join(["PRICE" + str(i+1).zfill(3) + "=VALUES(PRICE" + str(i+1).zfill(3) + ")" for i in range(inputdays)])
        #print(insert_sql)
        #print(insert_val)
        try:
            mycursor.executemany(insert_sql, insert_val)

            mydb.commit()    # 数据表内容有更新，必须使用到该语句
        except Exception as e:
            print(datetime.datetime.now(), ": INSERT DB :" ,e)
            break
        current_index = current_index + 1

    print(datetime.datetime.now(), ": Success!")