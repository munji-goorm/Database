# 라이브러리 import
import requests
import json
import pymysql
import os
import sys

# url 입력

url = 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty?serviceKey=9EVeUgs2qfDAndxMrmsipei8IlVyfYJLrYhjRHc1P3O1vpnEcS%2BX7CJByyCd81%2FdfwKZ1efWvF1WSaDwYG6ApA%3D%3D&returnType=json&numOfRows=1000&pageNo=1&sidoName=%EC%A0%84%EA%B5%AD&ver=1.2'

count = 0
while 1:
    try:
        response = requests.get(url)
        break
    except requests.exceptions.Timeout:
        print("Timeout Error.. Retry")
        count += 1
    except ConnectionRefusedError:
        print("Cannot connect to server.. Retry")
        count += 1
    finally:
        if (count >= 5):
            print("Try 5 times, but it doesn't work. Stop program")
            sys.exit()

contents = response.text

# 환경변수 불러오기
host=os.environ.get('DB_HOST')  # ex) '127.0.0.1'
port=int(os.environ.get('DB_PORT')) # ex) 3306
user=os.environ.get('DB_USER') # ex) 'root'
password=os.environ.get('DB_PW') # ex) '1234'
database=os.environ.get('DB_NAME') # ex) 'ApiExplorer'

# 문자열을 json으로 변경
json_ob = json.loads(contents)

# 필요한 내용만 꺼내기
body = json_ob['response']['body']['items']

if (len(body) != 0):
    # connection 정보. 접속
    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,  # ex) root
        password=password,
        database=database,
        charset='utf8'
    )

    # Cursor Object 가져오기
    curs = conn.cursor()
    curs.execute("TRUNCATE rt_air_info")

    for a in body:
        mangName = a['mangName']
        stationName = a['stationName']
        sidoName = a['sidoName']
        dataTime = a['dataTime']
        so2Value = a['so2Value'] if (a['so2Flag'] is None) else -1
        coValue = a['coValue'] if (a['coFlag'] is None) else -1
        pm10Value = a['pm10Value'] if a['pm10Flag'] is None else -1
        pm25Value = a['pm25Value'] if a['pm25Flag'] is None else -1
        no2Value = a['no2Value'] if a['no2Flag'] is None else -1
        o3Value = a['o3Value'] if a['o3Flag'] is None else -1
        khaiValue = a['khaiValue'] if (
            a['khaiValue'] != '-') and (a['khaiValue'] is not None) else -1

        sql = "INSERT INTO rt_air_info(so2_value, pm10_value, pm25_value, mang_name, station_name, no2_value, khai_value, co_value, sido_name, data_time, o3_value) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (float(so2Value), int(pm10Value), int(pm25Value), str(mangName), str(stationName), float(
            no2Value), int(khaiValue), float(coValue), str(sidoName), str(dataTime), float(o3Value))
        curs.execute(sql, val)
    
    curs.close()
    conn.commit()
    conn.close()
    print("record inserted")