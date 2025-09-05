from fingertips_import import get_fingertips_indicators 
#import sqlalchemy as sql
#import pyodbc
import pandas as pd

# get area codes from db
#conn = "mssql+pyodbc://MLCSU-BI-SQL/EAT_Reporting_BSOL?driver=SQL+Server+Native+Client+11.0&trusted_connection=yes"

#engine = sql.create_engine('mssql+pyodbc://@' + 'MLCSU-BI-SQL' + '/' + 'EAT_REPORTING_BSOL' + '?trusted_connection=Yes&driver=ODBC+Driver+17+for+SQL+Server')
 
#engine = sql.create_engine(conn)
 
#sql_query = """
#SELECT [aggregation_id]
#      ,[aggregation_type]
#      ,[aggregation_code]
#      ,[aggregation_label]
#  FROM [EAT_Reporting_BSOL].[OF].[OF2_Reference_Geography]
#  Where aggregation_type not in ('Locality (resident)', 'Locality (registered)', 'Unknown', 'All', 'Ward')
#"""

#area_lookup = pd.read_sql_query(sql_query, engine)

#area_codes = area_lookup['aggregation_code'].unique().tolist()

#sql_query2 = """
#Select distinct GPPracticeCode_Original
#from EAT_Reporting_BSOL.Reference.BSOL_ICS_PracticeMapped T2
#WHere T2.ICS_2223 = 'BSOL' 
#"""

#gp_lookup = pd.read_sql_query(sql_query2, engine)

gp_codes = ["M81062", "M85001", "M85002", "M85003", "M85005", "M85006", "M85007", "M85008", "M85009", "M85011", "M85013", "M85014", "M85015", "M85016", "M85018", "M85019", "M85020",
            "M85021", "M85023", "M85024", "M85025", "M85026", "M85027", "M85028", "M85029", "M85030", "M85031", "M85033", "M85034", "M85035", "M85036", "M85037", "M85038", "M85041",
            "M85042", "M85043", "M85045", "M85046", "M85047", "M85048", "M85051", "M85052", "M85053", "M85055", "M85056", "M85058", "M85059", "M85060", "M85061", "M85062", "M85063",
            "M85065", "M85066", "M85068", "M85069", "M85070", "M85071", "M85074", "M85075", "M85076", "M85077", "M85078", "M85079", "M85081", "M85082", "M85083", "M85084", "M85086",
            "M85087", "M85088", "M85092", "M85094", "M85097", "M85098", "M85103", "M85105", "M85107", "M85108", "M85110", "M85111", "M85113", "M85115", "M85116", "M85117", "M85118",
            "M85123", "M85124", "M85127", "M85128", "M85131", "M85134", "M85136", "M85139", "M85141", "M85142", "M85143", "M85145", "M85146", "M85149", "M85153", "M85154", "M85155",
            "M85156", "M85158", "M85159", "M85163", "M85164", "M85166", "M85167", "M85170", "M85171", "M85172", "M85174", "M85175", "M85176", "M85177", "M85178", "M85179", "M85600",
            "M85624", "M85634", "M85642", "M85652", "M85669", "M85670", "M85671", "M85676", "M85677", "M85679", "M85680", "M85684", "M85686", "M85689", "M85693", "M85694", "M85697",
            "M85699", "M85701", "M85704", "M85706", "M85711", "M85713", "M85715", "M85716", "M85717", "M85721", "M85722", "M85730", "M85732", "M85733", "M85735", "M85736", "M85738",
            "M85739", "M85741", "M85746", "M85749", "M85750", "M85753", "M85756", "M85757", "M85759", "M85766", "M85770", "M85773", "M85774", "M85776", "M85778", "M85779", "M85781",
            "M85782", "M85783", "M85784", "M85786", "M85791", "M85792", "M85794", "M85797", "M85801", "M85803", "M88020", "M89001", "M89002", "M89003", "M89004", "M89005", "M89006",
            "M89007", "M89008", "M89009", "M89010", "M89011", "M89012", "M89013", "M89015", "M89016", "M89017", "M89019", "M89020", "M89021", "M89023", "M89024", "M89026", "M89027",
            "M89028", "M89030", "M89601", "M89602", "M89606", "M89608", "M89609", "M91642", "Y00075", "Y00159", "Y00213", "Y00412", "Y00471", "Y00492", "Y01057", "Y01068", "Y02567",
            "Y02568", "Y02571", "Y02615", "Y02620", "Y02794", "Y02893", "Y02988", "Y03597", "Y05665", "Y05826", "Y06378", "Y08124"] 


#areas = list_area_types()

# Example indicator IDs
#ids = [90362, 11001, "bad_id"]

# Fist line is CSU tables from v1, second and third is BCC pipeline from v1
of_25 = [212, 219, 262, 280, 90647, 253, 90933, 93790, 91215, 
811, 20101, 20401, 30307, 41203, 90619, 91340, 91743, 92254, 92517, 92601, 92781, 93085, 
93088, 93183, 93605, 93675, 91041, 22001, 90631]

# 7 GP, 204 PCN, 221 ICB
#area_ids = [204, 221]

# Fetch data, no GP
#data = get_fingertips_indicators(of_25, area_ids, area_codes)

# Fetch data, no GP
data_GP = get_fingertips_indicators(of_25, [7], gp_codes)

# Union together
#combined = pd.concat([data, data_GP], ignore_index=True)


# INdex file with date for test
today_date = datetime.now().strftime('%Y%m%d')

data_GP.to_csv(f'./data/test_data_GP_{today_date}.csv', index=False)