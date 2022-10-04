
import json
import psycopg2
import boto3
import csv
import os
import sys

def lambda_handler(event, context):

    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(host="mypostgresdb.cyyv6lswmayt.us-east-1.rds.amazonaws.com", database="my_db", user="postgres", password="HanSolo98*")
    # create a cursor
    cur = conn.cursor()

    #CREATE TABLES 
    query_create_schema = "   CREATE TABLE jobs (id integer PRIMARY KEY,\
                                 job varchar);\
                                \
                              CREATE TABLE departments (id integer PRIMARY KEY, \
                                                        department varchar);\
                                \
                              CREATE TABLE hired_employees (id integer PRIMARY KEY, \
                                                            name varchar, \
                                                            datetime varchar, \
                                                            department_id integer, \
                                                            job_id integer,\
                                                            FOREIGN KEY (department_id) REFERENCES departments(id),\
                                                            FOREIGN KEY (job_id) REFERENCES jobs(id));   "
    
    try:
        cur.execute(query_create_schema)
        cur.close()
        conn.commit()
        print("The tables were created.")
    except:
        print("Error: The tables were already created.")
    
    # DOWNLOADING CSV FILES FROM S3
    bucket = 'jd-practice-bucket'
    key1 = 'data/jobs.csv'
    key2 = 'data/departments.csv'
    key3 = 'data/hired_employees.csv'
    download_path1 = '/tmp/jobs.csv'
    download_path2 = '/tmp/departments.csv'
    download_path3 = '/tmp/hired_employees.csv'
    s3_client = boto3.client('s3')
    s3_client.download_file(bucket, key1, download_path1)
    s3_client.download_file(bucket, key2, download_path2)
    s3_client.download_file(bucket, key3, download_path3)
    csv_data1 = csv.reader(open(download_path1,encoding='utf-8-sig'))
    csv_data2 = csv.reader(open(download_path2,encoding='utf-8-sig'))
    csv_data3 = csv.reader(open(download_path3,encoding='utf-8-sig'))

    
    #INSERTING ROWS INTO EACH TABLE
    with conn.cursor() as cur:
        
        # JOBS TABLE
        for idx, row in enumerate(csv_data1):
            try:
                cur.execute('INSERT INTO jobs (id, job)'\
                   'VALUES(%s, %s)', row)
            except Exception as e:
                print(e)
        conn.commit()
        
        # DEPARTMENTS TABLE
        for idx, row in enumerate(csv_data2):
            try:
                cur.execute('INSERT INTO departments (id, department)'\
                   'VALUES(%s, %s)', row)
            except Exception as e:
                print(e)
        conn.commit()
        
        # HIRED_EMPLOYEES
        for idx, row in enumerate(csv_data3):
            try:
                cur.execute('INSERT INTO hired_employees (id, name, datetime, department_id, job_id)'\
                   'VALUES(%s, %s, %s, %s, %s)', row)
            except Exception as e:
                print(e)
        conn.commit()