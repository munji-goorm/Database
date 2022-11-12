# 라이브러리 import

import pymysql
import csv


def insertsql_from_json():
    f = open('location2.csv', 'r', encoding='utf-8')
    rdr = csv.reader(f)
    # for line in rdr:
    # print(float(line[3]))
    # for line in rdr:
    # print(line[3])
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

    for a in rdr:
        if a[1] == '0':
            continue
        elif a[1] == '':
            fulladdr = (a[0] + ' '+a[2])
            shortaddr = a[2]
        else:
            fulladdr = (a[0]+' ' + a[1]+' '+a[2])
            shortaddr = (a[1]+' '+a[2])

        sql = "INSERT INTO korea_loc(full_addr, short_addr, x_coord, y_coord) VALUES (%s,%s,%s,%s)"
        val = (fulladdr, shortaddr, float(a[3]), float(a[4]))

        curs.execute(sql, val)
        conn.commit()

    f.close()


insertsql_from_json()
