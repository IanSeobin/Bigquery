# AU DISTINCT 문제 해결 아직 안됨 > 6.18 지금은 to_sql UPDATE 문제 해결부터..
# 일단 GROUP BY 에 FORMAT_DATETIME('{2}', timestamp) 빼도 AU가 DISTINCT하게 나오지 않는 점 확인할 것 
# SUMMARY 컬럼들은 잘나옴

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
  dense_rank() OVER(PARTITION BY case when is_active_user is true then user_pseudo_id end ORDER BY event_date) visit_count
, case when is_active_user is true then user_pseudo_id end is_active_user
, TIMESTAMP_ADD(TIMESTAMP_MICROS(event_timestamp), INTERVAL 9 HOUR) timestamp
, case when event_name = 'page_view' and key = 'page_location'  then 1 else 0 end as PAGE_VIEW
, concat(user_pseudo_id,(select value.int_value from unnest(event_params) where event_name = 'session_start' and key = 'ga_session_id')) SESSION_ID
, case when 
      ROW_NUMBER() 
      OVER(PARTITION BY 
        case when (
        select value.string_value from unnest(event_params) where key='session_engaged') = '1' 
        then concat(user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id')) end
      ORDER BY 
        event_timestamp,
        case when (select value.string_value from unnest(event_params) where key='session_engaged') = '1' 
        then concat(user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id')) end
      ) = 1
  then concat(user_pseudo_id, (select value.int_value from unnest(event_params) where key = 'ga_session_id')) END ENGAGED_SESSIONS
from
`skb-biz-ga4.analytics_320354291.events_*`, unnest(event_params)
where _TABLE_SUFFIX between '{0}' and '{1}'
)
select
case when visit_count = 1 then '1'
     when visit_count = 2 then '2'
     when visit_count = 3 then '3'
     when visit_count = 4 then '4'
     when visit_count = 5 then '5'
     when visit_count = 6 then '6'
     when visit_count = 7 then '7'
     when visit_count = 8 then '8'
     when visit_count = 9 then '9'
     when visit_count > 9 then '10_over' end visit_count
, MIN(CONCAT(REPLACE(FORMAT_DATETIME('%F', timestamp),'-', ''),'0000')) fromTIME, MAX(CONCAT(REPLACE(FORMAT_DATETIME('%F', timestamp),'-', ''),'2359')) toTIME
, sum(AU) AU, sum(PAGE_VIEW) PAGE_VIEW, sum(SESSIONS) SESSIONS, sum(ENGAGED_SESSIONS) ENGAGED_SESSIONS
from
  (
  select 
   timestamp
  , max(visit_count) visit_count, COUNT(DISTINCT is_active_user) AU, SUM(PAGE_VIEW) PAGE_VIEW, COUNT(DISTINCT SESSION_ID) SESSIONS, COUNT(DISTINCT ENGAGED_SESSIONS) ENGAGED_SESSIONS
  from TEMPORARY
  group by timestamp, is_active_user
  )
group by visit_count, FORMAT_DATETIME('{2}', timestamp)
order by 2, 1
""".format(int(sys.argv[1]), int(sys.argv[2]), str(sys.argv[3]))

query = client_BQ.query(sql)
data = query.to_dataframe()
data.to_sql('visit_count', con=db_connection, if_exists='replace', index=False)