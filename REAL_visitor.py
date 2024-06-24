# 데이터 내역: 추출자가 원하는 금일 시간(분) 간격으로 업데이트 되는 첫방문 및 재방문별 AU, PV, 세션수, 평균세션시간, 상담전화 수(X), 회원가입 수(X), 바로가입 수(X)

# sys.argv[1]: 추출자가 원하는 시간(분) 입력
# ex) 30분: python REAL_visitor.py 30
# ex) 2시간: python REAL_visitor.py 120

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
WITH REAL_visitor AS (
select  
  dense_rank() over(order by TIMESTAMP_SECONDS({0}*60 * DIV(UNIX_SECONDS(TIMESTAMP_ADD(TIMESTAMP_MICROS(event_timestamp), INTERVAL 9 HOUR)), {0}*60)) desc) rank
, case when (select value.int_value from unnest(event_params) where key = 'ga_session_number') = 1 then 'New' else 'Return' end newVSreturn
, TIMESTAMP_ADD(TIMESTAMP_MICROS(event_timestamp), INTERVAL 9 HOUR) timestamp
, user_pseudo_id AU 
, concat(user_pseudo_id,(select value.int_value from unnest(event_params) where key = 'ga_session_id')) SESSION_ID_ALL
, case when event_name = 'page_view' and key = 'page_location'  then 1 else 0 end as PAGE_VIEW
from `skb-home-ga4.analytics_319633764.events_intraday_*`, unnest(event_params)
where _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
)
select
REPLACE(FORMAT_DATETIME('%F%H%M',time),'-','') fromTIME, 
REPLACE(FORMAT_DATETIME('%F%H%M',timestamp_add(time, interval {0}*60 second)),'-','') toTIME
, newVSreturn
, sum(AU) AU, sum(PAGE_VIEW) PAGE_VIEW, count(distinct SESSION_ID_ALL) SESSIONS,sum(session_second)/count(distinct SESSION_ID_ALL) SESSION_AVG_DUR
from 
  (
  select
  newVSreturn, TIMESTAMP_SECONDS({0}*60 * DIV(UNIX_SECONDS(timestamp), {0}*60)) time, SESSION_ID_ALL
  , count(distinct AU) AU, sum(PAGE_VIEW) PAGE_VIEW
  , (UNIX_MICROS(max(timestamp))-UNIX_MICROS(min(timestamp)))/1000000 session_second
  from REAL_visitor
  where rank <> 1
  group by 1,2,3
  )
group by 1,2,3
order by 1 desc,3
""".format(int(sys.argv[1]))

query = client_BQ.query(sql)
data = query.to_dataframe()
data.to_sql('REAL_visitor', con=db_connection, if_exists='replace', index=False)