# 라이브러리 import
import requests
import json
import pymysql
import os
import sys

# url 입력
url = 'http://apis.data.go.kr/B552584/MsrstnInfoInqireSvc/getMsrstnList?serviceKey=9EVeUgs2qfDAndxMrmsipei8IlVyfYJLrYhjRHc1P3O1vpnEcS%2BX7CJByyCd81%2FdfwKZ1efWvF1WSaDwYG6ApA%3D%3D&returnType=json&numOfRows=1000&pageNo=1'

# API를 통해 데이터를 긁어오기
# 5회 에러 발생 시 파일 종료
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

    curs = conn.cursor()
    sql = "CREATE TABLE IF NOT EXISTS air_station_info(station_name varchar(15) NOT NULL PRIMARY KEY, addr varchar(100) NOT NULL, x_coord double NOT NULL, y_coord double NOT NULL)"
    curs.execute(sql)

    for a in body:    
        if a['stationName'] and a['addr'] and a['dmX'] and a['dmY']:
            sql = "INSERT INTO air_station_info(station_name, addr, x_coord, y_coord) VALUES (%s,%s,%s,%s)"
            val = (a['stationName'], a['addr'], float(a['dmX']), float(a['dmY']))
            curs.execute(sql,val)
        else:            
            continue
    curs.close()
    conn.commit()
    conn.close()
    print("record inserted")