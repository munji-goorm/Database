# 라이브러리 import
import requests
import json
import pymysql

# url 입력

url = 'http://apis.data.go.kr/B552584/MsrstnInfoInqireSvc/getMsrstnList?serviceKey=9EVeUgs2qfDAndxMrmsipei8IlVyfYJLrYhjRHc1P3O1vpnEcS%2BX7CJByyCd81%2FdfwKZ1efWvF1WSaDwYG6ApA%3D%3D&returnType=json&numOfRows=1000&pageNo=1'


try:
    response = requests.get(url)
except requests.exceptions.ConnectTimeout:
    response = requests.get(url)
except ConnectionRefusedError:
    print("서버에 연결할 수 없습니다.")
contents = response.text


# 문자열을 json으로 변경
json_ob = json.loads(contents)


# 필요한 내용만 꺼내기
body = json_ob['response']['body']['items']

if (len(body) != 0):
    def insertsql_from_json():
        # connection 정보

        # 접속
        # 비밀번호가 포함되어 있기 때문에 보통 config파일에서 key값으로 부른다.
        conn = pymysql.connect(
            host="localhost",  # ex) '127.0.0.1'
            port=3306,
            user="root",  # ex) root
            password="jm19980630!",
            database="ApiExplorer",
            charset='utf8'
        )
        # Cursor Object 가져오기
        curs = conn.cursor()
        for a in body:

            if a['stationName'] is not None:
                station_name = a['stationName']
            if a['addr'] is not None:
                addr = a['addr']
            if a['dmX'] is not None:
                dm_x = a['dmX']
            if a['dmY'] is not None:
                dm_y = a['dmY']

            sql = "INSERT INTO air_station_info(station_name, addr, x_coord, y_coord) VALUES (%s,%s,%s,%s)"
            val = (str(station_name), str(addr), float(dm_x), float(dm_y))

            curs.execute(sql, val)
            conn.commit()
        print("record inserted")
    insertsql_from_json()
