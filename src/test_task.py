import random
import base64
import hashlib
import string
import csv

import requests
import mysql.connector
from pymongo import MongoClient


def task_1():
    # 1.A
    file_name = 'data.csv'
    with open(file_name, 'w') as f:
        for _ in range(1024):
            row = [''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(8)) for _ in range(6)]
            f.write(','.join(row) + '\n')

    # 1.B
    file_name_edited = 'data_edited.csv'
    with open(file_name) as src, open(file_name_edited, 'w') as dest:
        csv_reader = csv.reader(src)
        for row in csv_reader:
            skip = False

            for i, col in enumerate(row):
                if col[0] in 'aoiue':
                    skip = True
                    break

                for d in "13579":
                    col = col.replace(d, '#')
                row[i] = col

            if not skip:
                dest.write(','.join(row) + '\n')

    # 1.C
    host = ''
    user = ''
    password = ''
    database = "Test_task"

    with mysql.connector.connect(host=host, user=user, password=password) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""create database if not exists {database}""")
        conn.commit()

    with mysql.connector.connect(host=host, user=user, password=password, database=database) as conn:
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute(f"""create table csv_data(
                            col1 varchar(8),
                            col2 varchar(8),
                            col3 varchar(8),
                            col4 varchar(8),
                            col5 varchar(8),
                            col6 varchar(8));""")

            with open(file_name) as src:
                csv_reader = csv.reader(src)
                query = "insert into csv_data (col1, col2, col3, col4, col5, col6) VALUES (%s, %s, %s, %s, %s, %s)"
                cur.executemany(query, list(map(tuple, list(csv_reader))))

            cur.execute("delete from csv_data where col2 rlike '[0-9].*'")

    # 1.D
    db = MongoClient().test_task_db

    with open(file_name) as src:
        csv_reader = csv.reader(src)

        for row in csv_reader:
            db.csv_data.insert_one(dict(zip(['col1', 'col2', 'col3', 'col4', 'col5', 'col6'], row)))

    db.csv_data.delete_many({"col3": {"$regex": r'^\D.*'}})


def task_2(url):
    # 2.A
    pic = requests.get(url).content

    file_name = hashlib.md5(pic).hexdigest()
    with open(file_name, 'wb') as f:
        f.write(base64.b64encode(pic))

    # 2.B
    with open(file_name, 'rb') as src, open(file_name + '.jpg', 'wb') as dest:
        dest.write(base64.b64decode(src.read()))


if __name__ == '__main__':
    task_1()
    task_2('https://images.theconversation.com/files/297893/original/file-20191021-56215-1wq7k71.jpg')
