import requests
import json
import pymysql
import sys
import os
from datetime import datetime, timedelta

# 문자열 처리
# data: 예보 ("서울 : 낮음, 인천 : 낮음, ..., 제주 : 낮음, 신뢰도 : 보통")
# date: 날짜 ("2022-11-17")
# 결과: 배열에 {'city': '서울', 'status': '낮음', 'date': '2022-11-17'}, ... 같은 형태로 값 저장해서 반환
def frcstdict(data, date):
    arr = []
    data, trust = data.split(', 신뢰도 : ')
    for i in data.split(', '):
        city, status = i.split(' : ')
        if city in {'강원영서', '경기남부'}:
            continue
        elif len(city) == 4:
            city = city[:2]

        if trust == '높음':
            if status == '높음':
                status = '최악'
            else:
                status = '좋음'
        else:
            if status == '높음':
                status = '나쁨'
            else:
                status = '보통'
        dict = {}
        dict['city'] = city
        dict['status'] = status
        dict['date'] = date
        arr.append(dict)
    return arr
    
yesterday = datetime.now() - timedelta(1)  # 어제 날짜
yyday = datetime.now() - timedelta(3)  # 3일전 날짜
today = datetime.now()  # 오늘 날짜

# url 입력
url = 'http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMinuDustWeekFrcstDspth?serviceKey=9EVeUgs2qfDAndxMrmsipei8IlVyfYJLrYhjRHc1P3O1vpnEcS%2BX7CJByyCd81%2FdfwKZ1efWvF1WSaDwYG6ApA%3D%3D&returnType=json&numOfRows=100&pageNo=1&searchDate='
frcst1 = url + str(yesterday.date())
frcst2 = url + str(yyday.date())

# API를 통해 데이터를 긁어오기
# 5회 에러 발생 시 파일 종료
count = 0
while 1:
    try:
        response1 = requests.get(frcst1)
        response2 = requests.get(frcst2)
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

# 환경변수 불러오기
host=os.environ.get('DB_HOST')  # ex) '127.0.0.1'
port=int(os.environ.get('DB_PORT')) # ex) 3306
user=os.environ.get('DB_USER') # ex) 'root'
password=os.environ.get('DB_PW') # ex) '1234'
database=os.environ.get('DB_NAME') # ex) 'ApiExplorer'

contents1 = response1.text
contents2 = response2.text

# 문자열을 json으로 변경
json_ob1 = json.loads(contents1)
json_ob2 = json.loads(contents2)

# 필요한 내용만 꺼내기
body1 = json_ob1['response']['body']['items']
body2 = json_ob2['response']['body']['items']

if (len(body1) != 0) and (len(body2) != 0):
    frcstArr = []
    frcstArr += frcstdict(body2[0]['frcstOneCn'], body2[0]['frcstOneDt'])
    frcstArr += frcstdict(body2[0]['frcstTwoCn'], body2[0]['frcstTwoDt'])
    frcstArr += frcstdict(body1[0]['frcstOneCn'], body1[0]['frcstOneDt'])
    frcstArr += frcstdict(body1[0]['frcstTwoCn'], body1[0]['frcstTwoDt'])
    frcstArr += frcstdict(body1[0]['frcstThreeCn'], body1[0]['frcstThreeDt'])
    frcstArr += frcstdict(body1[0]['frcstFourCn'], body1[0]['frcstFourDt'])

    # connection 정보 및 접속
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
    sql = "CREATE TABLE rt_air_forecast(id int AUTO_INCREMENT PRIMARY KEY, date_time varchar(30), city varchar(10), status varchar(10))"
    curs.execute(sql)
    curs.execute("TRUNCATE rt_air_forecast")

    for frcst in frcstArr:
        sql_insert = "INSERT INTO rt_air_forecast(date_time, city, status) VALUES (%s,%s,%s)"
        val = (frcst['date'], frcst['city'], frcst['status'])
        curs.execute(sql_insert, val)

    curs.close()    
    conn.commit()
    conn.close()
    print("record inserted")
#
# 만약 파일을 받아오지 못하면 어떻게 할지 고민..
# else:
#     os.execl(sys.executable, sys.executable, *sys.argv)