# 라이브러리 import
import requests
import json
import pymysql
import os

# url 입력
url = 'http://apis.data.go.kr/B552584/MsrstnInfoInqireSvc/getMsrstnList?serviceKey=9EVeUgs2qfDAndxMrmsipei8IlVyfYJLrYhjRHc1P3O1vpnEcS%2BX7CJByyCd81%2FdfwKZ1efWvF1WSaDwYG6ApA%3D%3D&returnType=json&numOfRows=1000&pageNo=1'

try:
    response = requests.get(url)
except requests.exceptions.ConnectTimeout:
    response = requests.get(url)
except ConnectionRefusedError:
    print("서버에 연결할 수 없습니다.")
contents = response.text

# 환경변수 불러오기
host=os.environ.get('DB_HOST')  # ex) '127.0.0.1'
port=int(os.environ.get('DB_PORT')) # ex) 3306
user=os.environ.get('DB_USER') # ex) 'root'
password=os.environ.get('DB_PW') # ex) '1234'
database=os.environ.get('DB_NAME') # ex) 'ApiExplorer'
charset='utf8'

# 문자열을 json으로 변경
json_ob = json.loads(contents)

# 필요한 내용만 꺼내기
body = json_ob['response']['body']['items']

if (len(body) != 0):
    # connection 정보. 접속
    # 비밀번호가 포함되어 있기 때문에 보통 config파일에서 key값으로 부른다.
    conn = pymysql.connect(
        host=host,
        port=int(port),
        user=user,  # ex) root
        password=password,
        database=database,
        charset='utf8'
    )
    
    curs = conn.cursor()
    sql_rows = []

    for a in body:
        if a['stationName'] and a['addr'] and a['dmX'] and a['dmY']:
            sql_row = "({},{},{},{})".format(a['stationName'], a['addr'], float(a['dmX']), float(a['dmY']))
            sql_rows.append(sql_row)
        else:            
            continue

    for i in range(7):
        sql = "INSERT INTO air_station_info(station_name, addr, x_coord, y_coord) VALUES " + ",".join(sql_rows[i*100:(i+1)*100])
        curs.execute(sql)
    curs.close()    
    conn.commit()
    conn.close()
    print("record inserted")
