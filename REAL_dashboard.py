# 데이터 내역: 금일 1시간 마다 업데이트 되는 사용자수, 조회수, 세션수, 메시지수(X)_현재 추가 불가능

from sqlalchemy import create_engine
import glob
import sys
from google.cloud import bigquery
from google.oauth2 import service_account

# 서비스 계정 키 json 파일 경로
key_path = glob.glob('./GA_KEY/skb-home-ga4-44e5a11751e6.json')[0]
# Credentials 객체 생성
credentials = service_account.Credentials.from_service_account_file(key_path)
# GCP 클라이언트 객체 생성
client_BQ = bigquery.Client(credentials = credentials, project = credentials.project_id)
# 데이터베이스 연결(mysql)
db_connection_path = 'mysql+pymysql://croi_statistics:q1w2e3r4t5@192.168.0.126:3306/croi_statistics'
db_connection = create_engine(db_connection_path)

sql = """ 
WITH REAL_dashboard AS (
select
dense_rank() over(order by fromTIME desc) rank, *
from
  (
  SELECT  
    CONCAT(REPLACE(FORMAT_DATETIME('%F%H', TIMESTAMP_ADD(TIMESTAMP_MICROS(event_timestamp), INTERVAL 9 HOUR)),'-',''),'00') fromTIME
  , CONCAT(REPLACE(FORMAT_DATETIME('%F%H', TIMESTAMP_ADD(TIMESTAMP_MICROS(event_timestamp), INTERVAL 9 HOUR)),'-',''),'59') toTIME
  , count(distinct user_pseudo_id) AU
  , sum(case when event_name = 'page_view' and key = 'page_location'  then 1 else 0 end) PAGE_VIEW
  , count(distinct concat(user_pseudo_id,(select value.int_value from unnest(event_params) where event_name = 'session_start' and key = 'ga_session_id'))) SESSIONS
  FROM `skb-home-ga4.analytics_319633764.events_intraday_*`, unnest(event_params)
  WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
  group by 1,2
  )
)
select
fromTIME, toTIME, AU, PAGE_VIEW, SESSIONS
from REAL_dashboard
where rank <> 1
order by 1 desc
"""

query = client_BQ.query(sql)
data = query.to_dataframe()
data.to_sql('REAL_dashboard', con=db_connection, if_exists='replace', index=False)

