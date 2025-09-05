from fingertips_import import get_fingertips_indicators 
import sqlalchemy as sql
import pyodbc
import pandas as pd

# get area codes from db
conn = "mssql+pyodbc://MLCSU-BI-SQL/EAT_Reporting_BSOL?driver=SQL+Server+Native+Client+11.0&trusted_connection=yes"

#engine = sql.create_engine('mssql+pyodbc://@' + 'MLCSU-BI-SQL' + '/' + 'EAT_REPORTING_BSOL' + '?trusted_connection=Yes&driver=ODBC+Driver+17+for+SQL+Server')
 
engine = sql.create_engine(conn)
 
sql_query = """
SELECT [aggregation_id]
      ,[aggregation_type]
      ,[aggregation_code]
      ,[aggregation_label]
  FROM [EAT_Reporting_BSOL].[OF].[OF2_Reference_Geography]
  Where aggregation_type not in ('Locality (resident)', 'Locality (registered)', 'Unknown', 'All', 'Ward')
"""

area_lookup = pd.read_sql_query(sql_query, engine)

area_codes = area_lookup['aggregation_code'].unique().tolist()

sql_query2 = """
Select distinct GPPracticeCode_Original
from EAT_Reporting_BSOL.Reference.BSOL_ICS_PracticeMapped T2
WHere T2.ICS_2223 = 'BSOL' 
"""

gp_lookup = pd.read_sql_query(sql_query2, engine)

gp_codes = gp_lookup['GPPracticeCode_Original'].unique().tolist()


#areas = list_area_types()

# Example indicator IDs
#ids = [90362, 11001, "bad_id"]

# Fist line is CSU tables from v1, second and third is BCC pipeline from v1
of_25 = [212, 219, 262, 280, 90647, 253, 90933, 93790, 91215, 
811, 20101, 20401, 30307, 41203, 90619, 91340, 91743, 92254, 92517, 92601, 92781, 93085, 
93088, 93183, 93605, 93675, 91041, 22001, 90631]

# 7 GP, 204 PCN, 221 ICB
area_ids = [204, 221]

# Fetch data, no GP
data = get_fingertips_indicators(of_25, area_ids, area_codes)

# Fetch data, no GP
data_GP = get_fingertips_indicators(of_25, 7, gp_codes)

# Union together
combined = pd.concat([data, data_GP], ignore_index=True)


# INdex file with date for test
today_date = datetime.now().strftime('%Y%m%d')

combined.to_csv(f'./data/test_data_{today_date}.csv', index=False)