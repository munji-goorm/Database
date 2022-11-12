import requests
import json
import pymysql
from datetime import datetime, timedelta


yesterday = datetime.now() - timedelta(1)  # 어제 날짜
yyday = datetime.now() - timedelta(3)  # 3일전 날짜
today = datetime.now()  # 오늘 날짜

# url 입력

frcst = 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMinuDustWeekFrcstDspth?serviceKey=9EVeUgs2qfDAndxMrmsipei8IlVyfYJLrYhjRHc1P3O1vpnEcS%2BX7CJByyCd81%2FdfwKZ1efWvF1WSaDwYG6ApA%3D%3D&returnType=json&numOfRows=100&pageNo=1&searchDate=' + \
    str(yesterday.date())
frcst2 = 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMinuDustWeekFrcstDspth?serviceKey=9EVeUgs2qfDAndxMrmsipei8IlVyfYJLrYhjRHc1P3O1vpnEcS%2BX7CJByyCd81%2FdfwKZ1efWvF1WSaDwYG6ApA%3D%3D&returnType=json&numOfRows=100&pageNo=1&searchDate=' + \
    str(yyday.date())

# 한 번만 실행하지말고 계속하게 해야된다.
try:
    response = requests.get(frcst)
    response2 = requests.get(frcst2)
except requests.exceptions.ConnectTimeout:
    response = requests.get(frcst)
    response2 = requests.get(frcst2)
except ConnectionRefusedError:
    print("서버에 연결할 수 없습니다.")

contents = response.text
contents2 = response2.text

# 문자열을 json으로 변경
json_ob = json.loads(contents)
json_ob2 = json.loads(contents2)


# 필요한 내용만 꺼내기
body = json_ob['response']['body']['items']
body2 = json_ob2['response']['body']['items']


if (len(body) != 0):
    def frcstdict(val1, val2):
        for f in val1.split(', '):
            x, y = f.split(' : ')
            if x == '신뢰도' or x == '강원영서' or x == '경기남부':
                continue
            elif x == '강원영동':
                x = '강원'
            elif x == '경기북부':
                x = '경기'
            dict = {}
            dict['city'] = x
            dict['status'] = y
            dict['date'] = val2
            frcstArr.append(dict)

    def frcst2dict(val3, val4):
        for f in val3.split(', '):
            x, y = f.split(' : ')
            if x == '신뢰도' or x == '강원영서' or x == '경기남부':
                continue
            elif x == '강원영동':
                x = '강원'
            elif x == '경기북부':
                x = '경기'
            dict = {}
            dict['city'] = x
            dict['status'] = y
            dict['date'] = val4
            frcstArr2.append(dict)

    frcstArr = []
    frcstArr2 = []

    frcstdict(body[0]['frcstOneCn'], body[0]['frcstOneDt'])
    frcstdict(body[0]['frcstTwoCn'], body[0]['frcstTwoDt'])
    frcstdict(body[0]['frcstThreeCn'], body[0]['frcstThreeDt'])
    frcstdict(body[0]['frcstFourCn'], body[0]['frcstFourDt'])

    frcst2dict(body2[0]['frcstOneCn'], body2[0]['frcstOneDt'])
    frcst2dict(body2[0]['frcstTwoCn'], body2[0]['frcstTwoDt'])

    def insertsql_from_json():
        # connection 정보
        # 접속.
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
        sql3 = "TRUNCATE rt_air_forecast"
        curs.execute(sql3)

        for b in frcstArr2:

            sql2 = "INSERT INTO rt_air_forecast(date_time,city,status) VALUES (%s,%s,%s)"
            val2 = (str(b['date']), str(
                b['city']), str(b['status']))

            curs.execute(sql2, val2)

        for a in frcstArr:

            sql = "INSERT INTO rt_air_forecast(date_time,city,status) VALUES (%s,%s,%s)"
            val = (str(a['date']), str(
                a['city']), str(a['status']))

            curs.execute(sql, val)
            conn.commit()

    insertsql_from_json()
