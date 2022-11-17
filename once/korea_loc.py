# 라이브러리 import
import pymysql
import csv
import os

f = open('location2.csv', 'r', encoding='utf-8')
rdr = csv.reader(f)

# 환경변수 불러오기
host=os.environ.get('DB_HOST')  # ex) '127.0.0.1'
port=int(os.environ.get('DB_PORT')) # ex) 3306
user=os.environ.get('DB_USER') # ex) 'root'
password=os.environ.get('DB_PW') # ex) '1234'
database=os.environ.get('DB_NAME') # ex) 'ApiExplorer'

# 접속
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

# Table 초기화
sql = "CREATE TABLE IF NOT EXISTS korea_loc(full_addr varchar(30) NOT NULL PRIMARY KEY, short_addr varchar(30) NOT NULL, x_coord double NOT NULL, y_coord double NOT NULL)"
curs.execute(sql)
curs.execute("TRUNCATE korea_loc")

for a in rdr:
    if a[1] == '0':
        continue
    elif a[1] == '':
        fulladdr = (a[0] + ' ' + a[2])
        shortaddr = a[2]
    else:
        fulladdr = (a[0] + ' ' + a[1] + ' ' + a[2])
        shortaddr = (a[1] + ' ' + a[2])

    sql = "INSERT INTO korea_loc(full_addr, short_addr, x_coord, y_coord) VALUES (%s,%s,%s,%s)"
    val = (fulladdr, shortaddr, float(a[3]), float(a[4]))

    curs.execute(sql, val)

curs.close()
conn.commit()
conn.close()
f.close()

print("record inserted")
