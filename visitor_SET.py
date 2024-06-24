# 데이터 내역: 설정한 기간(시작 시간, 마지막 시간) 동안의 활성사용자수(AU), 평균세션시간

# sys.argv[1]: 원하는 추출 시작 날짜(일)  ex) 20240501
# sys.argv[2]: 원하는 추출 마지막 날짜(일) ex) 20240505

# ex) 시간 별: python REAL_region.py 20240501 20240505

from sqlalchemy import create_engine
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
A.fromTIME, A.toTIME, AU, SESSION_AVG_DUR
FROM
    (
    select
      MIN(CONCAT(REPLACE(FORMAT_DATETIME('%F%H', timestamp),'-', ''),'00')) fromTIME
    , MAX(CONCAT(REPLACE(FORMAT_DATETIME('%F%H', timestamp),'-', ''),'59')) toTIME
    , COUNT(DISTINCT AU) AU
    from TEMPORARY
    ) A JOIN
    (
    select
    min(fromTIME) fromTIME, max(toTIME) toTIME, sum(session_second)/count(distinct SESSION_ID_ALL) SESSION_AVG_DUR
    from
        (
        select
          SESSION_ID_ALL
        , MIN(CONCAT(REPLACE(FORMAT_DATETIME('%F%H', timestamp),'-', ''),'00')) fromTIME
        , MAX(CONCAT(REPLACE(FORMAT_DATETIME('%F%H', timestamp),'-', ''),'59')) toTIME
        , (UNIX_MICROS(max(timestamp))-UNIX_MICROS(min(timestamp)))/1000000 session_second
        from TEMPORARY
        group by 1
        )
    ) B ON A.fromTIME = B.fromTIME AND A.toTIME = B.toTIME
""".format(int(sys.argv[1]), int(sys.argv[2]))

query = client_BQ.query(sql)
data = query.to_dataframe()
data.to_sql('visitor_SET', con=db_connection, if_exists='replace', index=False)
