# workflow test
# 라이브러리 import
import requests
import json
import pymysql
import os
import sys

# url 입력
url = 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty?serviceKey=9EVeUgs2qfDAndxMrmsipei8IlVyfYJLrYhjRHc1P3O1vpnEcS%2BX7CJByyCd81%2FdfwKZ1efWvF1WSaDwYG6ApA%3D%3D&returnType=json&numOfRows=1000&pageNo=1&sidoName=%EC%A0%84%EA%B5%AD&ver=1.2'

# api 호출
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

# 문자열을 json으로 변경
json_ob = json.loads(contents)

# 필요한 내용만 꺼내기
body = json_ob['response']['body']['items']

def get_state(value, flag):
    if value < 0:
        return "점검중"
    standard = {"khai": [50, 100, 250], "pm10K": [30, 80, 150], "pm10W": [30, 50, 100], "pm25K": [15, 35, 75], "pm25W": [15, 25, 50], 
                "o3": [0.03, 0.09, 0.15], "co": [2.0, 9.0, 15.0], "no2": [0.03, 0.06, 0.2], "so2": [0.02, 0.05, 0.15]}

    if value <= standard[flag][0]:
        return "좋음"
    elif value <= standard[flag][1]:
        return "보통"
    elif value <= standard[flag][2]:
        return "나쁨"
    else:
        return "최악"

# 환경변수 불러오기
host = os.environ.get('DB_HOST')  # ex) '127.0.0.1'
port = int(os.environ.get('DB_PORT'))  # ex) 3306
user = os.environ.get('DB_USER')  # ex) 'root'
password = os.environ.get('DB_PW')  # ex) '1234'
database = os.environ.get('DB_NAME')  # ex) 'ApiExplorer'

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
    sql = "CREATE TABLE IF NOT EXISTS rt_air_info(so2_value Float, so2_state varchar(6), pm10_value int, pm10_stateK varchar(6), pm10_stateW varchar(6), pm25_value int, pm25_stateK varchar(6), pm25_stateW varchar(6), mang_name varchar(15), station_name varchar(15) PRIMARY KEY, no2_value Float, no2_state varchar(6), khai_value int, khai_state varchar(6), co_value Float, co_state varchar(6), sido_name varchar(15), data_time varchar(30), o3_value Float, o3_state varchar(6))"
    curs.execute(sql)
    curs.execute("TRUNCATE rt_air_info")

    for a in body:
        mang_name = a['mangName']
        station_name = a['stationName']
        sido_name = a['sidoName']
        data_time = a['dataTime']
        so2_value = float(a['so2Value']) if (
                    (a['so2Flag'] is None) and (a['so2Value'] is not None)) else -1
        so2_state = get_state(so2_value, 'so2')
        co_value = float(a['coValue']) if (
                    (a['coFlag'] is None) and (a['coValue'] is not None)) else -1
        co_state = get_state(co_value, 'co')
        pm10_value = int(a['pm10Value']) if (
                    (a['pm10Flag'] is None) and (a['pm10Value'] is not None)) else -1
        pm10_stateK = get_state(pm10_value, 'pm10K')
        pm10_stateW = get_state(pm10_value, 'pm10W')
        pm25_value = int(a['pm25Value']) if (
                    (a['pm25Flag'] is None) and (a['pm25Value'] is not None)) else -1
        pm25_stateK = get_state(pm25_value, 'pm25K')
        pm25_stateW = get_state(pm25_value, 'pm25W')
        no2_value = float(a['no2Value']) if (
                    (a['no2Flag'] is None) and (a['no2Value'] is not None)) else -1
        no2_state = get_state(no2_value, 'no2')
        o3_value = float(a['o3Value']) if (
                    (a['o3Flag'] is None) and (a['o3Value'] is not None)) else -1
        o3_state = get_state(o3_value, 'o3')
        khai_value = int(a['khaiValue']) if (
                    a['khaiValue'] != '-') and (a['khaiValue'] is not None) else -1
        khai_state = get_state(khai_value, 'khai')

        sql = "INSERT INTO rt_air_info(so2_value, so2_state, pm10_value, co_state, pm25_value, pm10_stateK, pm10_stateW, pm25_stateK, pm25_stateW, mang_name, no2_state, o3_state, khai_state, station_name, no2_value, khai_value, co_value, sido_name, data_time, o3_value) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (so2_value, so2_state, pm10_value, co_state, pm25_value, pm10_stateK, pm10_stateW, pm25_stateK, pm25_stateW, mang_name, no2_state, o3_state, khai_state, station_name, 
            no2_value, khai_value, co_value, sido_name, data_time, o3_value)
        curs.execute(sql, val)

    curs.close()
    conn.commit()
    conn.close()
    print("record inserted")
