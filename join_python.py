import mysql.connector, json
from mysql.connector import Error


mydb = mysql.connector.connect(
  host="localhost",
  user="sandeepj",
  passwd="sandeepj!@#",
  database="test"



abc = (tbl_pmaster.merge(tbl_spec_display, on='product_id', how='left')
         .groupby(['product_id','product_name','catid','catname'])['spec_id','spec_display_value']
         .apply(lambda x: [dict(x.values)])
         .reset_index(name='spec_info')
         .to_dict(orient='records')
)
#print (d)

import json
json = json.dumps({'data':abc})
