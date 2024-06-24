# 데이터 내역: 추출자가 원하는 금일 시간(분) 간격으로 업데이트 되는 device category별 사용자수

# sys.argv[1]: 추출자가 원하는 시간(분) 입력
# ex) 30분: python REAL_device.py 30
# ex) 2시간: python REAL_device.py 120

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
WITH REAL_device AS (
SELECT
dense_rank() over(order by time desc) rank, 
REPLACE(FORMAT_DATETIME('%F%H%M',time),'-','') fromTIME, 
REPLACE(FORMAT_DATETIME('%F%H%M',timestamp_add(time, interval {0}*60 second)),'-','') toTIME, category, AU
FROM
  (
  SELECT  
    TIMESTAMP_SECONDS({0}*60 * DIV(UNIX_SECONDS(TIMESTAMP_ADD(TIMESTAMP_MICROS(event_timestamp), INTERVAL 9 HOUR)), {0}*60)) time
  , device.category
  , count(distinct user_pseudo_id) AU
  FROM `skb-home-ga4.analytics_319633764.events_intraday_*`, unnest(event_params)
  WHERE _TABLE_SUFFIX = FORMAT_DATE('%Y%m%d',DATE_SUB(CURRENT_DATE(), INTERVAL 0 DAY))
  group by 1, 2
  )
)
SELECT
fromTIME, toTIME, category, AU
FROM REAL_device
WHERE rank <> 1
order by 1 desc, 4 desc
""".format(int(sys.argv[1]))

query = client_BQ.query(sql)
data = query.to_dataframe()
data.to_sql('REAL_device', con=db_connection, if_exists='replace', index=False)