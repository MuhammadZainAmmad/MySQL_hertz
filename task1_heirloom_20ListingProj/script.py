import mysql.connector
import os

conn = mysql.connector.connect(
    host="104.155.129.36",
    user="etl",
    password=os.environ["heirloom_pass"],
    # password = 'uH)6f_MtQ6@g!=Sr',
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
                'Annual Projection' AS ColName,
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
                MIN(`Annual Projection`),
                MAX(`Annual Projection`),
                AVG(`Annual Projection`),
                SUM(`Annual Projection`),
                MIN(`Market_Monthly_Proj`),
                MAX(`Market_Monthly_Proj`),
                AVG(`Market_Monthly_Proj`),
                SUM(`Market_Monthly_Proj`),
                MIN(`Projected Rev %`),
                MAX(`Projected Rev %`),
                AVG(`Projected Rev %`),
                SUM(`Projected Rev %`),
                MIN(`Projected Occupancy`),
                MAX(`Projected Occupancy`),
                AVG(`Projected Occupancy`),
                SUM(`Projected Occupancy`),
                MIN(`Monthly Proj`),
                MAX(`Monthly Proj`),
                AVG(`Monthly Proj`),
                SUM(`Monthly Proj`),
                MIN(`Final Monthly Projection`),
                MAX(`Final Monthly Projection`),
                AVG(`Final Monthly Projection`),
                SUM(`Final Monthly Projection`),
                MIN(`Market_Monthly_Proj_New`),
                MAX(`Market_Monthly_Proj_New`),
                AVG(`Market_Monthly_Proj_New`),
                SUM(`Market_Monthly_Proj_New`)
            FROM 20_Listing_Proj
            GROUP BY Market;
        """
]

responses = []
for query in sql_queries: 
    cursor.execute(query)
    rows = cursor.fetchall()
    responses.append(rows)
    

for res in responses:
    print(res)


cursor.close()
conn.close()
