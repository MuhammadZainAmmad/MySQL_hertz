import mysql.connector
import os
import pandas as pd
from slack_sdk import WebClient

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
