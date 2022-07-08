from osisoft.pidevclub.piwebapi.pi_web_api_client import PIWebApiClient
import pandas as pd
import json
import pyodbc
pyodbc.pooling = False

with open('config.json') as f:
    config = json.load(f)

PI_WEB_API_URL = 'https://ing-pgh1-picp1/piwebapi'
VERIFY_SSL = False 
client = PIWebApiClient(PI_WEB_API_URL, useKerberos=True, verifySsl=VERIFY_SSL)

paths = ['pi:\\ing-wic2-pihp1\CP-TruckScale'];
dfs2 = client.data.get_multiple_recorded_values(paths, start_time="*-1d", end_time= "*", selected_fields="items.items.value")
df = dfs2['pi:\\ing-wic2-pihp1\\CP-TruckScale'].Value.str.split("|", expand=True)
df.columns = ['Date', 'Truck Number', 'Material Number', 'Vendor Number', 'Net Weight', 'Vendor Name', 'Material;', 'Ticket #']

# with pd.ExcelWriter(r'C:\Users\aw92\OneDrive - Ingevity\Documents\Truck Scale.xlsx') as writer:
#         df.to_excel(writer, sheet_name='Scale Data')


config = config["ING-PGH1-PIDP1"]
server = config['server']
database = config['database']
username = config['username']
password = config['password']  
driver= '{ODBC Driver 17 for SQL Server}'
conn_str = 'DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password

with pyodbc.connect(conn_str) as conn:
    cursor = conn.cursor()
    for i, row in df.iterrows():
        cursor.execute("INSERT INTO AF_PublishDB.PI.TruckScale ([Date], [Truck Number], [Material Number], [Vendor Number], [Net Weight], [Vendor Name], Material, [Ticket #]) VALUES(?, ?, ?, ?, ?, ?, ?, ?);", row.Date, row['Truck Number'], row['Material Number'], row['Vendor Number'], row['Net Weight'], row['Vendor Name'], row['Material'], row['Ticket #'])
    conn.commit()