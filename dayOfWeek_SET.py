from sqlalchemy import create_engine
import pymysql
import glob
import sys
from google.cloud import bigquery
from google.oauth2 import service_account

# 서비스 계정 키 json 파일 경로
key_path = glob.glob('./GA_KEY/skb-biz-ga4-7ecd7ac6662a.json')[0]
# Credentials 객체 생성
credentials = service_account.Credentials.from_service_account_file(key_path)
# GCP 클라이언트 객체 생성
client_BQ = bigquery.Client(credentials = credentials, project = credentials.project_id)
# 데이터베이스 연결(mysql)
db_connection_path = 'mysql+pymysql://croi_statistics:q1w2e3r4t5@192.168.0.126:3306/croi_statistics'
db_connection = create_engine(db_connection_path)

sql = """ 
WITH TEMPORARY AS (
select
  TIMESTAMP_ADD(TIMESTAMP_MICROS(event_timestamp), INTERVAL 9 HOUR) timestamp
, case when is_active_user is true then user_pseudo_id end AU
, concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) SESSION_ID_ALL
from
`skb-biz-ga4.analytics_320354291.events_*`, unnest(event_params)
where _TABLE_SUFFIX between '{0}' and '{1}'
)
SELECT
A.DayOfweek, MIN(A.fromTIME) fromTIME, MAX(A.toTIME) toTIME, AVG(AU) AU, AVG(SESSION_AVG_DUR) SESSION_AVG_DUR
FROM
    (
    select
      REPLACE(FORMAT_DATETIME('%F', timestamp),'-', '') time
    , CASE WHEN EXTRACT(DAYOFWEEK from timestamp) = 1 THEN '일'
           WHEN EXTRACT(DAYOFWEEK from timestamp) = 2 THEN '월'
           WHEN EXTRACT(DAYOFWEEK from timestamp) = 3 THEN '화'
           WHEN EXTRACT(DAYOFWEEK from timestamp) = 4 THEN '수'
           WHEN EXTRACT(DAYOFWEEK from timestamp) = 5 THEN '목'
           WHEN EXTRACT(DAYOFWEEK from timestamp) = 6 THEN '금'
           WHEN EXTRACT(DAYOFWEEK from timestamp) = 7 THEN '토' END DayOfweek
    , MIN(CONCAT(REPLACE(FORMAT_DATETIME('%F', timestamp),'-', ''),'0000')) fromTIME
    , MAX(CONCAT(REPLACE(FORMAT_DATETIME('%F', timestamp),'-', ''),'2359')) toTIME
    , COUNT(DISTINCT AU) AU
    from TEMPORARY
    group by 1, 2
    ) A JOIN
    (
    select
    time, DayOfweek, min(fromTIME) fromTIME, max(toTIME) toTIME, sum(session_second)/count(distinct SESSION_ID_ALL) SESSION_AVG_DUR
    from 
        (
        select
          REPLACE(FORMAT_DATETIME('%F', timestamp),'-', '') time, SESSION_ID_ALL
        , CASE WHEN EXTRACT(DAYOFWEEK from timestamp) = 1 THEN '일'
               WHEN EXTRACT(DAYOFWEEK from timestamp) = 2 THEN '월'
               WHEN EXTRACT(DAYOFWEEK from timestamp) = 3 THEN '화'
               WHEN EXTRACT(DAYOFWEEK from timestamp) = 4 THEN '수'
               WHEN EXTRACT(DAYOFWEEK from timestamp) = 5 THEN '목'
               WHEN EXTRACT(DAYOFWEEK from timestamp) = 6 THEN '금'
               WHEN EXTRACT(DAYOFWEEK from timestamp) = 7 THEN '토' END DayOfweek
        , MIN(CONCAT(REPLACE(FORMAT_DATETIME('%F%H', timestamp),'-', ''),'00')) fromTIME
        , MAX(CONCAT(REPLACE(FORMAT_DATETIME('%F%H', timestamp),'-', ''),'59')) toTIME
        , (UNIX_MICROS(max(timestamp))-UNIX_MICROS(min(timestamp)))/1000000 session_second
        from TEMPORARY
        group by 1, 2, 3
        )
    group by 1, DayOfweek
    ) B ON A.fromTIME = B.fromTIME AND A.toTIME = B.toTIME AND A.DayOfweek = B.DayOfweek
GROUP BY DayOfweek
""".format(int(sys.argv[1]), int(sys.argv[2]))

query = client_BQ.query(sql)
data = query.to_dataframe()
data.to_sql('dayOfWeek_SET', con=db_connection, if_exists='replace', index=False)

