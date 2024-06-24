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
  case when key='page_location' then value.string_value end page
, TIMESTAMP_ADD(TIMESTAMP_MICROS(event_timestamp), INTERVAL 9 HOUR) timestamp
, case when event_name = 'page_view' and key = 'page_location'  then 1 else 0 end as PAGE_VIEW
, concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) SESSION_ID
, case when (select value.string_value from unnest(event_params) where key = 'session_engaged') = '1'
       then (select value.int_value from unnest(event_params) where key = 'ga_session_id') end ENGAGED_SESSIONS
from
`skb-biz-ga4.analytics_320354291.events_*`, unnest(event_params)
where _TABLE_SUFFIX between '{0}' and '{1}'
)
select
page, fromTIME, toTIME, sum(PAGE_VIEW) PAGE_VIEW, sum(SESSIONS) SESSIONS, sum(ENGAGED_SESSIONS) ENGAGED_SESSIONS
from
  (
  select
    REPLACE(FORMAT_DATETIME('%F%H', timestamp),'-', '')
  , case when page like '%?%' then substr(page, 1, instr(page, '?',1,1)-1) else page end page
  , MIN(CONCAT(REPLACE(FORMAT_DATETIME('%F%H', timestamp),'-', ''),'00')) fromTIME
  , MAX(CONCAT(REPLACE(FORMAT_DATETIME('%F%H', timestamp),'-', ''),'59')) toTIME
  , SUM(PAGE_VIEW) PAGE_VIEW, COUNT(DISTINCT SESSION_ID) SESSIONS, COUNT(DISTINCT ENGAGED_SESSIONS) ENGAGED_SESSIONS
  from TEMPORARY
  where page is not null
  group by 1,2
  )
group by 1,2,3
""".format(int(sys.argv[1]), int(sys.argv[2]))

query = client_BQ.query(sql)
data = query.to_dataframe()
data.to_sql('page_SUM', con=db_connection, if_exists='replace', index=False)