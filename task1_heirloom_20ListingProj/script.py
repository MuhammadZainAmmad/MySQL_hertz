import mysql.connector
import os
import pandas as pd
from slack_sdk import WebClient

# imports for sending html page on gmail
import base64
from email.mime.text import MIMEText
from googleapiclient.discovery import build
from google.oauth2 import service_account

# MySQL connection
conn = mysql.connector.connect(
    host="104.155.129.36",
    user="etl",
    password=os.environ["heirloom_pass"],
    database="heirloom",
)
cursor = conn.cursor()

sql_queries = [
        """
            SELECT 
                COUNT(*) AS TotalRecords
            FROM 20_Listing_Proj;
        """,
        """
            SELECT
                'Annual Projection' AS Column_Name,
                MIN(`Annual Projection`) AS MinVal,
                MAX(`Annual Projection`) AS MaxVal,
                AVG(`Annual Projection`) AS AvgVal,
                SUM(`Annual Projection`) AS SumVal
            FROM 20_Listing_Proj
            UNION
            SELECT
                'Market_Monthly_Proj',
                MIN(Market_Monthly_Proj),
                MAX(Market_Monthly_Proj),
                AVG(Market_Monthly_Proj),
                SUM(Market_Monthly_Proj)
            FROM 20_Listing_Proj
            UNION
            SELECT
                'Projected Rev %',
                MIN(`Projected Rev %`),
                MAX(`Projected Rev %`),
                AVG(`Projected Rev %`),
                SUM(`Projected Rev %`)
            FROM 20_Listing_Proj
            UNION
            SELECT
                'Projected Occupancy',
                MIN(`Projected Occupancy`),
                MAX(`Projected Occupancy`),
                AVG(`Projected Occupancy`),
                SUM(`Projected Occupancy`)
            FROM 20_Listing_Proj
            UNION
            SELECT
                'Monthly Proj',
                MIN(`Monthly Proj`),
                MAX(`Monthly Proj`),
                AVG(`Monthly Proj`),
                SUM(`Monthly Proj`)
            FROM 20_Listing_Proj
            UNION
            SELECT
                'Final Monthly Projection',
                MIN(`Final Monthly Projection`),
                MAX(`Final Monthly Projection`),
                AVG(`Final Monthly Projection`),
                SUM(`Final Monthly Projection`)
            FROM 20_Listing_Proj
            UNION
            SELECT
                'Market_Monthly_Proj_New',
                MIN(Market_Monthly_Proj_New),
                MAX(Market_Monthly_Proj_New),
                AVG(Market_Monthly_Proj_New),
                SUM(Market_Monthly_Proj_New)
            FROM 20_Listing_Proj;
        """,
        """
            SELECT 
                DISTINCT Market,
                MIN(`Annual Projection`) AS Min_AnnualProjection,
                MAX(`Annual Projection`) AS Max_AnnualProjection,
                AVG(`Annual Projection`) AS Avg_AnnualProjection,
                SUM(`Annual Projection`) AS Total_AnnualProjection,
                MIN(`Market_Monthly_Proj`) AS Min_MarketMonthlyProj,
                MAX(`Market_Monthly_Proj`) AS Max_MarketMonthlyProj,
                AVG(`Market_Monthly_Proj`) AS Avg_MarketMonthlyProj,
                SUM(`Market_Monthly_Proj`) AS Total_MarketMonthlyProj,
                MIN(`Projected Rev %`) AS Min_ProjectedRevPercent,
                MAX(`Projected Rev %`) AS Max_ProjectedRevPercent,
                AVG(`Projected Rev %`) AS Avg_ProjectedRevPercent,
                SUM(`Projected Rev %`) AS Total_ProjectedRevPercent,
                MIN(`Projected Occupancy`) AS Min_ProjectedOccupancy,
                MAX(`Projected Occupancy`) AS Max_ProjectedOccupancy,
                AVG(`Projected Occupancy`) AS Avg_ProjectedOccupancy,
                SUM(`Projected Occupancy`) AS Total_ProjectedOccupancy,
                MIN(`Monthly Proj`) AS Min_MonthlyProj,
                MAX(`Monthly Proj`) AS Max_MonthlyProj,
                AVG(`Monthly Proj`) AS Avg_MonthlyProj,
                SUM(`Monthly Proj`) AS Total_MonthlyProj,
                MIN(`Final Monthly Projection`) AS Min_FinalMonthlyProj,
                MAX(`Final Monthly Projection`) AS Max_FinalMonthlyProj,
                AVG(`Final Monthly Projection`) AS Avg_FinalMonthlyProj,
                SUM(`Final Monthly Projection`) AS Total_FinalMonthlyProj,
                MIN(`Market_Monthly_Proj_New`) AS Min_MarketMonthlyProjNew,
                MAX(`Market_Monthly_Proj_New`) AS Max_MarketMonthlyProjNew,
                AVG(`Market_Monthly_Proj_New`) AS Avg_MarketMonthlyProjNew,
                SUM(`Market_Monthly_Proj_New`) AS Total_MarketMonthlyProjNew
            FROM 20_Listing_Proj
            GROUP BY Market;
        """
]

# setup for slack
channel_id = "C05E96Q6PFB"
slack_token = os.environ['slackAPIToken_task1']
client = WebClient(token=slack_token)

# setup for gmail
def service_account_login():
    SCOPES = ['https://www.googleapis.com/auth/gmail.send']
    client_credentials = service_account.Credentials.from_service_account_info({
        "type": "service_account",
        "project_id": "stayloom",
        "private_key_id": "5b8181b3f0b5ffccf65a10f8a43adccb4f3d202d",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDKGRVqt6MqnLTI\nAO9fVzcgKPGljrDlTVwUP15IURAAen5HvcvTvYsLTg9K+5Dgv9q23ETPMie9m3TA\nKpINGq12QIDQrFTkXiWdlLTsTjrN87YnH+KB7k6xIN4e9/o5a22tBYseNzmJfi+N\nr7d5APi/bbcD/9VwYbuK2cVBH745/QTThB3orXPnoeBl4iXu1uxzuf4BERbQN7Kp\ng5gmMl3nJrJzcQL4Dm4zH42JCD1uyr6r+kPnSb4/V4P1vZsFeBHlNK/B8kgl6g4f\nvGuQODF43Ht8sAYnYwGskh/vBwYcTe7VxgGpQT125s6nhFPl/heeoa6Eksh1JnTN\nfiXZQXTVAgMBAAECggEABGfS4L8gLABh0rKHuIbkvhgUzN4rfiAqcVjAfY6VoCql\nBvujJ+c/BfZqaaD9k3l5maVh5mtkB7BBHAq0uttM3zuAo18YwNt9i4eCzY9W0FQ8\nMXLZCiopMuRlLmMk/dK/jHdG64tA8mQ0dXmYZA3XN+/hS4TGJ06pTjMfmryuQqjQ\neuyfYZWlOHJx9PY2hhn1WxsopjSZzHNkqh8cOECc0EI2EstMX0V97e/CH9Gr+AEh\n7fDvczudsgQMmA6DTrmwWpRBWSzG0BS8hVg+Rb0wIQHNvVMSFLJzF5tV1Rq1Utwi\n3ADAur17PmgQk2TUHvlZnTFzKDLkmLR+KPMOxyYLkQKBgQDr9+tNJ+EsfT0i8hpH\nq71+KuBU30ckuVKiqu7RWqS2vZc+yd8GDeZAkZHg/NOer/rZuRUY4SfQes/Wb7UR\nuTox5tfbGrAbvnYaVwPQRKR9EUmYrW/pIJ6RrQZz0kzax2nIvMWUWyxpbjtIQNvf\n7bTsr22uoGc0ow5StyoihizGEQKBgQDbQRb5KMJjAyHkeD0vw6BsIbZlJQqItRps\nLRp7GA/ODUC6Uzbyjc8MhUU8BTRTnX3PajTPiQBMdsfFuyl4Cz259Q+fJpYnMoCc\nmjyWB7vUxGgGOLFsXmB8teufIbpxJlhFAD6OK3NvQCMFaNEE3M/rvDiWOR2FUxYR\n3OmYlG+uhQKBgCt700uzlqYpKhP/g2JDvra6Vf6t6qFU2WqKj1nbF1FpnK6Aau3l\nr9GkQbqxPJoYmeR3W/DqxPiBOT2t9jMe7B94B70jrOJf9cmi0VwW2i0F+4b8JwxR\n64ay2OaNEYabit3oE4zkREnle100PpCEcHvRVCgC/SHRDnmlsUkNasDRAoGAf7U5\nCWXpW7yuWCKFGTYsUe+NCvr5WMmMG2hmHT7VreJgSmdAASYCbLuPqTcq1G1Oo6qs\nGholl2Q0VoL+05JQoOkR8VSLb0dmTFE2avkUOgkwwjbxeTq7nshj9uuxaki4b3CF\n/09lzG4iN/tmjBuF7DxVBYM9I7RSjZMMaThEmPUCgYAlRBbpbFfmU++OvRqhzJ9s\nCFKAQcfZ1z+ui4nYBWrIbxd0Tzp8fVzPALhsUFSQJ3EWzKvCoXYsCXsfiT8ZMbmz\nhioLdk4Cd5L4SGQBPVEflj93y02u+rkN2vp8wHa53W6i1PRSkhSfOR2FB9PQ+95J\nOB1wM9NbZHymbTRvKnW6ZA==\n-----END PRIVATE KEY-----\n",
        "client_email": "stayloom-etl-3@stayloom.iam.gserviceaccount.com",
        "client_id": "101951561882038605426",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/stayloom-etl-3%40stayloom.iam.gserviceaccount.com"
    }, )
    credentials = client_credentials.with_scopes(
        ['https://www.googleapis.com/auth/gmail.send']
    )
    delegated_credentials = credentials.with_subject('it@stayloom.com')
    service = build('gmail', 'v1', credentials=delegated_credentials)
    return service
service=service_account_login()

# Querying db, getting reult set in df, saving all response set in a list
list_responseSets = []
for query in sql_queries: 
    responseSet_df = pd.read_sql(query, conn)
    list_responseSets.append(responseSet_df)  
  
# sending each response set in a different message on slack    
for res in list_responseSets:    
    if len(res.columns)>5: # if result set has more than 5 columns (query3), split df on column axis
        df_market = res.iloc[:, 0] # separating first column (market)
        for i in range(1, len(res.columns), 4):
            df = pd.concat([df_market, res.iloc[:, i:i+4]], axis=1)
            markdown_table = df.to_markdown(index=False, tablefmt="grid")
            response = client.chat_postMessage(channel=channel_id, text= f"```{markdown_table}```")
    else:
        markdown_table = res.to_markdown(index=False, tablefmt="grid")
        response = client.chat_postMessage(channel=channel_id, text= f"```{markdown_table}```")

cursor.close()
conn.close()
