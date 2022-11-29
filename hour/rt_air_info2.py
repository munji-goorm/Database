# workflow test
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

# 문자열을 json으로 변경
json_ob = json.loads(contents)

# 필요한 내용만 꺼내기
body = json_ob['response']['body']['items']


def calStateInteger(value, type):
    if value < 0:
        return "점검중"

    if type == "khai":
        if value <= 50:
            return "좋음"
        elif value <= 100:
            return "보통"
        elif value <= 250:
            return "나쁨"
        else:
            return "최악"
    elif type == "pm10K":
        if value <= 30:
            return "좋음"
        elif value <= 80:
            return "보통"
        elif value <= 150:
            return "나쁨"
        else:
            return "최악"
    elif type == "pm10W":
        if value <= 30:
            return "좋음"
        elif value <= 50:
            return "보통"
        elif value <= 100:
            return "나쁨"
        else:
            return "최악"
    elif type == "pm25K":
        if value <= 15:
            return "좋음"
        elif value <= 35:
            return "보통"
        elif value <= 75:
            return "나쁨"
        else:
            return "최악"
    elif type == "pm25W":
        if value <= 15:
            return "좋음"
        elif value <= 25:
            return "보통"
        elif value <= 50:
            return "나쁨"
        else:
            return "최악"
    else:
        return ""


def calStateFloat(value, type):

    if value < 0:
        return "점검중"

    if type == "o3":
        if value <= 0.03:
            return "좋음"
        elif value <= 0.09:
            return "보통"
        elif value <= 0.15:
            return "나쁨"
        else:
            return "최악"
    elif type == "co":
        if value <= 2.0:
            return "좋음"
        elif value <= 9.0:
            return "보통"
        elif value <= 15.0:
            return "나쁨"
        else:
            return "최악"
    elif type == "no2":
        if value <= 0.03:
            return "좋음"
        elif value <= 0.06:
            return "보통"
        elif value <= 0.2:
            return "나쁨"
        else:
            return "최악"
    elif type == "so2":
        if value <= 0.02:
            return "좋음"
        elif value <= 0.05:
            return "보통"
        elif value <= 0.15:
            return "나쁨"
        else:
            return "최악"
    else:
        return ""


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
        so2_value = a['so2Value'] if (
            (a['so2Flag'] is None) and (a['so2Value'] is not None)) else -1
        so2_state = calStateFloat(float(so2_value), 'so2')
        co_value = a['coValue'] if ((a['coFlag'] is None) and (
            a['coValue'] is not None)) else -1
        co_state = calStateFloat(float(co_value), 'co')
        pm10_value = a['pm10Value'] if (
            (a['pm10Flag'] is None) and (a['pm10Value'] is not None)) else -1
        pm10_stateK = calStateInteger(int(pm10_value), 'pm10K')
        pm10_stateW = calStateInteger(int(pm10_value), 'pm10W')
        pm25_value = a['pm25Value'] if (
            (a['pm25Flag'] is None) and (a['pm25Value'] is not None)) else -1
        pm25_stateK = calStateInteger(int(pm25_value), 'pm25K')
        pm25_stateW = calStateInteger(int(pm25_value), 'pm25W')
        no2_value = a['no2Value'] if (
            (a['no2Flag'] is None) and (a['no2Value'] is not None)) else -1
        no2_state = calStateFloat(float(no2_value), 'no2')
        o3_value = a['o3Value'] if ((a['o3Flag'] is None) and (
            a['o3Value'] is not None)) else -1
        o3_state = calStateFloat(float(o3_value), 'o3')
        khai_value = a['khaiValue'] if (
            a['khaiValue'] != '-') and (a['khaiValue'] is not None) else -1
        khai_state = calStateInteger(int(khai_value), 'khai')

        sql = "INSERT INTO rt_air_info(so2_value, so2_state, pm10_value, co_state, pm25_value, pm10_stateK, pm10_stateW, pm25_stateK, pm25_stateW, mang_name, no2_state, o3_state, khai_state, station_name, no2_value, khai_value, co_value, sido_name, data_time, o3_value) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (float(so2_value), str(so2_state), int(pm10_value), str(co_state), int(pm25_value), str(pm10_stateK), str(pm10_stateW), str(pm25_stateK), str(pm25_stateW), str(mang_name), str(no2_state), str(o3_state), str(khai_state), str(station_name), float(
            no2_value), int(khai_value), float(co_value), str(sido_name), str(data_time), float(o3_value))
        curs.execute(sql, val)

    curs.close()
    conn.commit()
    conn.close()
    print("record inserted")
