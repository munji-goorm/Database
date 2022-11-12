# 라이브러리 import
import requests
import json
import pymysql

# url 입력

url = 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getCtprvnRltmMesureDnsty?serviceKey=9EVeUgs2qfDAndxMrmsipei8IlVyfYJLrYhjRHc1P3O1vpnEcS%2BX7CJByyCd81%2FdfwKZ1efWvF1WSaDwYG6ApA%3D%3D&returnType=json&numOfRows=1000&pageNo=1&sidoName=%EC%A0%84%EA%B5%AD&ver=1.2'

response = requests.get(url)

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
            conn.commit()

    insertsql_from_json()
