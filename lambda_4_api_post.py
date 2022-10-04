import json
import psycopg2

def lambda_handler(event, context):
    conn = psycopg2.connect(host="mypostgresdb.cyyv6lswmayt.us-east-1.rds.amazonaws.com", database="my_db", user="postgres", password="HanSolo98*")
    # create a cursor
    cur = conn.cursor()

    try:
        cur.execute("INSERT INTO hired_employees (id,name,datetime,department_id,job_id) VALUES("+str(event['id'])+",'"+str(event['name'])+"','"+str(event['datetime'])+"',"+str(event['department_id'])+","+str(event['job_id'])+")")
        conn.commit()
        return 'One row was inserted'
    except Exception as e:
        return e