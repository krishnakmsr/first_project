import sys
import threading
import MySQLdb
import MySQLdb.cursors
import MySQLdb.converters
from concurrent.futures import ThreadPoolExecutor
from collections import OrderedDict
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json
from collections import MutableMapping, MutableSequence
from decimal import Decimal
import collections
import smtplib
import time

# production | development
ENV = "production"
PROCESSING_SCRIPT_NAME = "Main_SFCronjob.py"

NEW_ACTIVE_THREADS = 3
LIMIT_PER_THREAD_ON_PENDING = 10000
LIMIT_PER_THREAD_ON_DELETE = 10000
# LIMIT_PER_THREAD_ON_NEW = 10000
LIMIT_PER_THREAD_ON_NEW = 5000
LIMIT_PER_VARIANT_BATCH = 10000

# Default credentials when ENV = development
HOST = "192.168.17.172"
NODES = ["localhost"]
USER = "sureshm"
PASS = "sureshm!@#"
DBNAME = "quick_quotes_live"
TABLENAME = "tbl_product_change_data_py"
KEYWORD_TABLENAME = "tbl_elastic_keyword_data"

# Credentials when ENV = production
if ENV == 'production':
    NODES = ["192.168.17.75", "192.168.17.74", "192.168.17.73", "192.168.24.247", "192.168.29.125"]
    USER = "quickquotes"
    PASS = "qu!ckqu0t3s"
    DBNAME = "quick_quotes_live"
    TABLENAME = "tbl_product_change_data"
    KEYWORD_TABLENAME = "tbl_elastic_keyword_data"

# Elasticsearch shopfront index details
ES_LIMIT_PER_REQUEST = 1000
ES_INDEX = "shopfront_old"
ES_TYPE = "products2"

# Elasticsearch keyword index details
ES_SF_KEYWORD = {
    'popular' : {
        'index' : 'shopfront_pop',
        'type' : 'shopfront_pop',
    },
    'unpopular' : {
        'index' : 'shopfront_unpop',
        'type' : 'shopfront_unpop',
    }
}

# Popular Category list
SF_POP_CATIDS = [1000800,1000335,1001424,1000040,1000093,1000635,1000094,1000548,1000672,1002584,1000536,1000440,1002439,1000471,1001146,1001408,1001376,1001274,1002579,1001456,1000346,1001331,1000086,1000598,1000885,1000603,1001448,1001449,1001450,1001446,1000265,1000589,1000613,1002474,1000361,1002473,1001369,1000087,1000476,1000521,1000586,1002421,1001421,1000768,1000628,1000350,1001447,1000480,1000024,1000267,1000390,1000979,1000064,1000763,1000259,1000071,1000324,1000644,1002497,1000251,1000933,1000062,1001315,1000068,1001188,1000649,1000726,1001258,1000101,1001145,1000620,1000798,1000164,1000322,1001343,1000526,1000667,1000278,1000289,1000566,1000016,1000630,1000293,1001283,1001273,1000397,1000380,1000379,1001070,1000700,1000381,1000701,1000679,1000626,1000539,1001386,1002477,1000022,1000060,1001240,1001269,1002472,1000077,1000743,1000096,1000041,1000047,1000524,1000530,1000283,1000764,1001275,1001413,1001326,1000133,1000610,1001249,1000556,1001420,1000348,1000602,1001264,1000069,1001025,1000569,1000025,1000119,1000026,1000042,1000388,1000543,1000260,1001219,1001416,1000031,1000032,1001415,1000765,1001267,1001388,1000349,1000473,1001028,1001403,1000577,1001252,1001295,1000321,1000330,1000839,1001418,1000724,1000286,1000043,1000525,1000399,1001378,1000820,1000936,1000974,1002578,1000991,1002489,1000744,1000746,1000121,1000284,1000017,1001335,1000551,1002581,1000544,1001414,1000949,1001286,1000677,1001271,1000821,1000252,1002576,1000395,1000131,1000052,1000792,1000257,1000255,1000012,1000562,1000641,1000076,1000132,1000006,1001205,1000745,1000347,1001278,1001322,1001380,1001015,1002575,1001404,1000054,1001406,1001319,1000794,1001329,1002606,1000769,1000280,1001156,1000518,1000980,1001370,1000465,1002475,1000973,1000038,1000039,1001236,1000479,1000055,1001377,1001262,1000545,1000056,1001419,1001261,1001036,1000546,1001445,1000034,1000382,1001263,1000394,1001347,1001425,1000523,1000831,1000717,1001407,1000360,1001371,1000262,1001265,1002455,1001372,1001260,1001332,1001102,1000663,1000454,1000755,1000522,1001101,1000117,1001214,1000387,1000352,1000383,1001387,1000163,1000061,1001390,1000013,1000742,1001266,1002494,1000058,1000542,1001344,1000864,1000384,1000678,1001207,1002602,1000462,1000075,1000982,1001244,1001285,1000044,1000297,1000345,1001033,1000600,1000089,1000045,1001333,1000004,1000537,1002454,1000108,1000472,1000351,1000875,1000385,1002577,1000396,1001284,1001391,1000799,1001184,1001246]


# Creating ES client for elasticsearch shopfront servers
def create_es_cluster_conn():
    try:        
        es_cluster = Elasticsearch(NODES, timeout=60, max_retries=10, retry_on_timeout=True)
        return es_cluster

    except Exception as exception_msg:
        print("Exception Caught when connecting to Elastic source server: ")
        print("Exception Message: ",exception_msg)
        return False


# fucntion for processing product ids with their keywords
def process_products_keyword(cursor, products_keyword, keyword_index, keyword_type):

    # fetch all product id keywords
    bulk_data = []
    pid_list = []

    for product in products_keyword:
        pid_list.append(product['pid'])

    try:
        pid_str =  ",".join(pid_list)
        fetchKeywordsFromTbl = "SELECT product_id,keyword FROM %s WHERE product_id IN (%s)" % (KEYWORD_TABLENAME, pid_str)
        cursor.execute(fetchKeywordsFromTbl)
        product_ids_result = cursor.fetchall()
        # print("product_ids_result: ", product_ids_result)
        
    except Exception as exception_msg:
        print("Exception Caught when fetching keywords from keyword table")
        print("Exception Mesaage: ",exception_msg)
        print("fetchKeywordsFromTbl: ",fetchKeywordsFromTbl)
        # sys.exit()
        return

    try:
        product_keywords_list = {}
        if len(product_ids_result) > 0:
            for keyword_elem in product_ids_result:                
                splited_keywords = keyword_elem['keyword'].split('|@|')
                elem_pid = keyword_elem['product_id']

                keywords_list = []
                for elem in splited_keywords:
                    if elem:
                        keywords_list.append({"key":elem})
                        
                if len(keywords_list) > 0:
                    product_keywords_list[elem_pid] = keywords_list

    except Exception as exception_msg:
        print("Exception Caught when processing keywords")
        print("Exception Mesaage: ",exception_msg)
        # sys.exit()
        return

    # Restructure product data with their respective keywords
    for product in products_keyword:
        if int(product['pid']) in product_keywords_list:
            product['keyword'] = product_keywords_list[int(product['pid'])]
        
        bulk_data.append({
            "index": {
                '_index': keyword_index,
                '_type': keyword_type,
                '_id': product['pid']
            }
        })

        bulk_data.append(product)

    # print("Bulk keyword data to index : ", json.dumps(bulk_data))

    # Reindex updated product docs
    try:
        es = Elasticsearch(NODES, timeout=60, max_retries=10, retry_on_timeout=True)
        responses = es.bulk(index=keyword_index, body=bulk_data, refresh=True)
        print("Keyword data indexed successfully")        
        
    except Exception as exception_msg:
        print("Exception Caught when indexing product keyword data")
        print("Exception Message: ",exception_msg)
        print("bulk_data: ", bulk_data)
        # sys.exit()
        return


# Function for deleting pids from elasticsearch index
def delete_keyword_pids(pid_chunk, keyword_index, keyword_type):

    # Get Source Server conn
    es_cluster = create_es_cluster_conn()

    if not es_cluster:
        print("Could not able to create connection to shopfront cluster")
        # sys.exit()
        return

    if len(pid_chunk) > 0:
        bulk_data = []

        for pid in pid_chunk:

            # Deleting pids from shopfront_pop index
            bulk_data.append({
                    "delete": {
                        '_index': keyword_index,
                        '_type': keyword_type,
                        '_id': pid
                    }
                })

            try:
                if len(bulk_data) == ES_LIMIT_PER_REQUEST:
                    responses = es_cluster.bulk(index=keyword_index, body=bulk_data, refresh=True)
                    bulk_data = []

            except Exception as exception_msg:
                print("Exception Caught when deleting documents from shopfront_pop index ")
                print("Exception Message: ",exception_msg)
                return
        
        # Deleting extra product ids from shopfront_pop index
        try:
            if len(bulk_data) > 0:
                responses = es_cluster.bulk(index=keyword_index, body=bulk_data, refresh=True)
                # print("Deleted Product IDs from keywords table")

        except Exception as exception_msg:
            print("Exception Caught when deleting extra documents from shopfront_pop index ")
            print("Exception Message: ",exception_msg)
            return


# Fucntion for getting mysql connection
def get_connection():
    # charset='utf8', use_unicode=True
    args = {
        'host': HOST,
        'user': USER,
        'passwd': PASS,
        'db': DBNAME,
        'use_unicode': True,
        'charset': 'utf8',
        'cursorclass': MySQLdb.cursors.DictCursor
    }

    try:
        connection = MySQLdb.connect(**args)
        connection.autocommit(True)
        return connection
    
    except Exception as exception_msg:

        try:
            sender = 'sf.elastic@justdial.com'
            receivers = ['suresh.majhi@justdial.com', 'irfan.mirza@justdial.com', 'meeta.bilimoria@justdial.com']

            receivers_str = ", ".join(receivers)
            email_subject = "MySQL Exception Caught"

            email_message = """From: Shopfront Elastic Module <%s>
To: To Person <%s>
Subject: %s

Server - http://192.168.17.75
File Path - http://192.168.17.75/cronpy/SFCronjob.py
MySQL Exception Caught - %s
""" % (sender, receivers_str, email_subject, exception_msg)

            # Sending emails
            smtpObj = smtplib.SMTP('localhost')
            smtpObj.sendmail(sender, receivers, email_message) 
        
        except SMTPException:
            print ("Error: Unable to send email")

        quit()


# Function for converting python data format
def convert_es_format(dict_data):
    if isinstance(dict_data, tuple):
        dict_data = list(dict_data)

    stack = [dict_data]
    while stack:
        json_data = stack.pop()
        if isinstance(json_data, MutableMapping):  # json object
            it = json_data.items()
        elif isinstance(json_data, MutableSequence):  # json array
            it = enumerate(json_data)
        else:  # scalar data
            continue

        for k, v in it:
            if isinstance(v, Decimal) or isinstance(v, int) or isinstance(v, long) or isinstance(v, float):
                json_data[k] = str(v)
            if v is None:
                json_data[k] = u''
            else:
                stack.append(v)


# Function for getting product spec list
def get_specs(cursor, catid, productIdsFromDb, cat_specs, variants, product_variant_ids_from_db):
    sql = 'SELECT table_num FROM tbl_mapping_lookup WHERE immediate_parent_catid= %d' % (catid)
    cursor.execute(sql)
    table_num = int(cursor.fetchone()['table_num'])
    pids = ','.join(productIdsFromDb)

    sql = "SELECT DISTINCT b.product_id, b.spec_id, b.spec_display_value, b.spec_numeric_value, b.catid, a.sort_spec_position, b.active_flag, " \
          "a.sort_active_flag, b.callcount, b.callcount_rolling, b.spec_cnt, 'f_spec' AS type  FROM (SELECT catid AS pcatid, spec_id, sort_spec_position, " \
          "sort_active_flag FROM tbl_spec_mapping WHERE catid IN (%d) ORDER BY FIELD(catid,%d), IF(spec_id=103,1,0) DESC, sort_spec_position ASC ) a JOIN " \
          "tbl_product_spec_%d b ON a.pcatid = b.catid AND a.spec_id = b.spec_id WHERE b.catid = %d AND product_id IN (%s)" % (
          catid, catid, table_num, catid, pids)

    cursor.execute(sql)
    spec_filter = cursor.fetchall()

    sql = "SELECT spec_id,spec_display_value,spec_numeric_value,product_id, 'd_spec' AS type, active_flag FROM tbl_product_spec_display " \
          "WHERE catid = %d AND product_id IN (%s)" % (catid, pids)
    cursor.execute(sql)
    spec_display = cursor.fetchall()

    spec_list = spec_filter + spec_display
    convert_es_format(spec_list)

    spec_display_array = {}
    convert_es_format(spec_display)

    for row in spec_display:
        if not spec_display_array.has_key(row['product_id']):
            spec_display_array[row['product_id']] = {}
            spec_display_array[row['product_id']][row['spec_id']] = {}

        spec_display_array[row['product_id']][row['spec_id']] = {"value": row['spec_display_value'], "s_dname": "", "s_unit": ""}

    spec_preview_array = {}
    spec_output_array = {}
    spec_positions = {}

    for pid in productIdsFromDb:
        for spec in cat_specs:
            if spec_display_array.has_key(pid) and spec_display_array.get(pid).has_key(spec['spec_id']) and spec_display_array.get(pid).get(spec['spec_id']).get('value'):

                spec_display_array[pid][spec['spec_id']]['s_dname'] = spec['spec_display_name']
                spec_display_array[pid][spec['spec_id']]['s_unit'] = spec['spec_unit']

                if not spec_positions.has_key(pid):
                    spec_positions[pid] = 0

                if not spec_preview_array.has_key(pid):
                    spec_preview_array[pid] = {}
                    

                if spec['preview_active_flag'] == '1':
                    if spec_positions[pid] < int(spec['preview_spec_position']):
                        spec_positions[pid] = int(spec['preview_spec_position'])

                    spec_preview_array[pid][spec['preview_spec_position']] = spec['spec_id'] + "|@|" + spec[
                        'spec_display_name'] + "|@|" + spec['spec_unit'] + "|@|" + \
                        spec_display_array[pid][spec['spec_id']]['value'] + "|@|" + spec['sort_active_flag']

                if not spec_output_array.has_key(pid):
                    spec_output_array[pid] = []

                    
                # spec_output_array[pid].append(spec['spec_id'] + "|@|" + spec['spec_display_name'] + "|@|" + spec['spec_unit'] + "|@|" + spec_display_array[pid][spec['spec_id']]['value'] + "|@|" + spec['sort_active_flag'])

                spec_output_array[pid].append(spec['spec_id'] + "|@|" + spec['spec_display_name'] + "|@|" + spec['spec_unit'] + "|@|" + spec_display_array[pid][spec['spec_id']]['value'] + "|@|" + spec['spec_unit_pos'])



    # print json.dumps(spec_preview_array)

    for pid in productIdsFromDb:
        if spec_preview_array.has_key(pid):
            last_index = spec_positions[pid]+1
            
            if len(spec_preview_array[pid].keys()) < 6:
                for item in spec_output_array[pid]:
                    #print item, spec_preview_array[pid].values()
                    if not item in spec_preview_array[pid].values():
                        spec_preview_array[pid][str(last_index)] = item

                        if len(spec_preview_array[pid].keys()) == 6:
                            break

                        last_index = last_index + 1
                    # else:
                    #     print item
    # print('spec_preview_array')
    # print json.dumps(spec_preview_array)
    # sys.exit()



    product_spec_list = {}

    if spec_list:
        spec_id_list = map(lambda x: x['spec_id'], spec_list)

        spec_id_list = list(OrderedDict.fromkeys(spec_id_list))

        sql = "SELECT spec_id,spec_name,spec_display_name, spec_unit, spec_numeric_flag, active_flag  FROM tbl_spec_master WHERE spec_id IN (%s) " % (
            ','.join(map(str, spec_id_list)))
        cursor.execute(sql)

        specInfo = cursor.fetchall()
        convert_es_format(specInfo)

        spec_info = {}
        for row in specInfo:
            spec_info[row['spec_id']] = row

        for row in spec_list:
            temp_spec = {'sid': row['spec_id'], 'sdv': row['spec_display_value'], 'snv': row['spec_numeric_value'],
                         'af': row['active_flag'], 'sn': '', 'sdn': '', 'su': '', 'snf': ''}

            if row['type'] == 'f_spec':
                temp_spec['ssp'] = row['sort_spec_position']
                temp_spec['saf'] = row['sort_active_flag']
                temp_spec['cc'] = row['callcount']
                temp_spec['ccr'] = row['callcount_rolling']
                temp_spec['sc'] = row['spec_cnt']

            if spec_info.has_key(row['spec_id']):
                temp_spec['sn'] = spec_info[row['spec_id']]['spec_name']
                temp_spec['sdn'] = spec_info[row['spec_id']]['spec_display_name']
                temp_spec['su'] = spec_info[row['spec_id']]['spec_unit']
                temp_spec['snf'] = spec_info[row['spec_id']]['spec_numeric_flag']

            if row['product_id'] in product_spec_list:
                product_spec_list[row['product_id']][row['type']].append(temp_spec)
            else:
                product_spec_list[row['product_id']] = {'d_spec': [], 'f_spec': [], 'dval': '', 'pval': '', 'pavid': '', 'variants': {}}
                product_spec_list[row['product_id']][row['type']].append(temp_spec)

    # for pid in spec_display_array.keys():
    #     product_spec_list[pid]['dval'] = '|~|'.join(spec_output_array[pid])

    #     temp_preview_array = []
    #     for key in sorted(spec_preview_array.get(pid).keys(), key=my_key):
    #         temp_preview_array.append(spec_preview_array.get(pid).get(key))

    #     product_spec_list[pid]['pval'] = '|~|'.join(temp_preview_array)
    
    for pid in spec_display_array.keys():
        product_spec_list[pid]['dval'] = '|~|'.join(spec_output_array[pid]) if spec_output_array.has_key(pid) else ''         

        if spec_preview_array.has_key(pid):
            temp_preview_array = []
            for key in sorted(spec_preview_array.get(pid).keys(), key=my_key):
                temp_preview_array.append(spec_preview_array.get(pid).get(key))

            product_spec_list[pid]['pval'] = '|~|'.join(temp_preview_array)
        else:
            product_spec_list[pid]['pval'] = ''

    sql = "SELECT product_id, GROUP_CONCAT(product_videopath ORDER BY Video_type ASC, product_videopath ASC separator '|~|') AS video_path " \
          "FROM tbl_product_video WHERE product_id IN (%s) AND active_flag=1 GROUP BY product_id ORDER BY video_type ASC, product_videopath ASC" % (pids)
    cursor.execute(sql)
    videos = cursor.fetchall()

    for row in videos:
        if product_spec_list.has_key(row['product_id']):
            product_spec_list[row['product_id']]['pavid'] = row['video_path']


    # Build all variants pid list
    variants_details = build_variant_data(cursor, productIdsFromDb, product_variant_ids_from_db, variants, spec_display_array)


    for pid in productIdsFromDb:

        variant_univ_specid = []
        variant_specid_val_mapping = {}
        variant_spec = []

        for row in variants:
            if spec_display_array.has_key(pid) and spec_display_array.get(pid).has_key(
                    row['spec_id']) and spec_display_array.get(pid).get(row['spec_id']).get('value'):
                variant_univ_specid.append(row['spec_id'])
                variant_specid_val_mapping[row['spec_id']] = row['spec_display_name']
                variant_spec.append({row['spec_display_name']: spec_display_array[pid][row['spec_id']]['value'],
                                     'total_variants': str(len(product_variant_ids_from_db[pid].split(',')) + 1)})

        variants_universal_specs = ",".join(variant_univ_specid)

        # if variant_spec:
        #     product_spec_list[pid]['variants'].update({'upfront': {'spec': variant_spec}})
        if variant_spec:
            if pid in variants_details['variants_info_details']:
                product_spec_list[pid]['variants'].update({'upfront': {'spec': variant_spec}})

        variant_prods = product_variant_ids_from_db[pid]

        if variant_prods and variants_universal_specs:

            #     sql = "SELECT product_displayname,a.product_id,jd_price,national_catid,images_crawled_path, product_imagepath FROM tbl_product_master a " \
            #           "JOIN (SELECT tbl_product_image.product_id,tbl_product_image.product_imagepath FROM tbl_product_image WHERE product_id " \
            #           "IN (%s) AND image_type=1 AND active_flag=1) t ON a.product_id = t.product_id WHERE a.product_id IN (%s)" % (variant_prods+","+pid, variant_prods+","+pid)
            #     cursor.execute(sql)
            #     img_details = cursor.fetchall()
            #
            #     variants_info_details = {}
            #     for row in img_details:
            #         variants_info_details[str(row['product_id'])] = {'pdisp': row['product_displayname'], 'jdguarantee_price': row['jd_price'], 'national_catid': row['national_catid'], 'product_id': row['product_id'], 'image': row['product_imagepath']}
            #
            #     sql = "SELECT product_id, group_concat(concat(concat(spec_display_value,'|!|'),spec_id) ORDER BY field (spec_id,%s) " \
            #           "SEPARATOR '|~|') AS spec_combined FROM tbl_product_spec_display WHERE product_id IN(%s) " \
            #           "AND spec_id IN (%s) GROUP BY product_id " % (variants_universal_specs, variant_prods+","+pid, variants_universal_specs)
            #     cursor.execute(sql)
            #     variants_specs = cursor.fetchall()
            #     convert_es_format(variants_info_details)
            #
            #     spec_indexes = {}
            #     for row in variants_specs:
            #         spec_indexes[row['spec_combined']] = row['product_id']

            # ---new code,start---
            variants_pids = []
            variants_pids.append(pid)
            splited_variant = product_variant_ids_from_db[pid].split(",")
            variants_pids = variants_pids + splited_variant

            variants_info_details = {}
            spec_indexes = {}

            for v_pid in variants_pids:
                if v_pid in variants_details['variants_info_details']:
                    details = variants_details['variants_info_details'][v_pid]
                    variants_info_details[v_pid] = {'pdisp': details['pdisp'],
                                                    'jdguarantee_price': details['jdguarantee_price'],
                                                    'national_catid': details['national_catid'],
                                                    'product_id': details['product_id'], 'image': details['image']}

                if v_pid in variants_details['spec_indexes']:
                    spec_indexes.update(variants_details['spec_indexes'][v_pid])
            # ---new code,end---

            convert_es_format(variants_info_details)
            convert_es_format(spec_indexes)

            variants_arr_final = {}
            for key in spec_indexes.keys():
                keyarr = key.split("|~|")
                cnt = len(keyarr)-1
                new_key_arr = {}

                while cnt >= 0:
                    key_split = keyarr[cnt].split("|!|")

                    new_key_arr_temp = {}

                    if isinstance(new_key_arr, dict) and new_key_arr:
                        new_key_arr_temp[key_split[0]] = new_key_arr
                    else:
                        if variants_info_details.has_key(spec_indexes[key]):
                            new_key_arr_temp[key_split[0]] = variants_info_details[spec_indexes[key]]
                        else:
                            new_key_arr_temp[key_split[0]] = None

                    new_key_arr = new_key_arr_temp
                    cnt-=1

                variants_arr_final = update(variants_arr_final, new_key_arr)

                # if product_spec_list.has_key(pid):
                #     product_spec_list[pid]['variants'].update({'details': {'specs': variants_arr_final}})
                if product_spec_list.has_key(pid):
                    if pid in variants_details['variants_info_details']:
                        product_spec_list[pid]['variants'].update({'details': {'specs': variants_arr_final}})


            sql = "SELECT product_id, spec_display_value,spec_id FROM tbl_product_spec_display WHERE product_id IN(%s) " \
                  "AND spec_id IN (%s) GROUP BY spec_id,spec_display_value " % (variant_prods+","+pid, variants_universal_specs)
            cursor.execute(sql)
            variants_universal = cursor.fetchall()
            convert_es_format(variants_universal)


            # new_univ_arr = {}
            # new_univ_arr = collections.OrderedDict()
            # for row in variants_universal:
            #     if not new_univ_arr.has_key(variant_specid_val_mapping[row['spec_id']]):
            #         new_univ_arr[variant_specid_val_mapping[row['spec_id']]] = []

            #     if row['spec_id'] == '2636' and variants_info_details.has_key(row['product_id']) and variants_info_details[row['product_id']]['image']:
            #         new_univ_arr[variant_specid_val_mapping[row['spec_id']]].append({row['spec_display_value']: variants_info_details[row['product_id']]['image']})
            #     else:
            #         new_univ_arr[variant_specid_val_mapping[row['spec_id']]].append({row['spec_display_value']: ""})


            # Build universal node of variants->details. First buld the color node & after that add all non color specific nodes
            new_univ_arr = collections.OrderedDict()
            for row in variants_universal:
                if row['spec_id'] == '2636':
                    if not new_univ_arr.has_key(variant_specid_val_mapping[row['spec_id']]):
                        new_univ_arr[variant_specid_val_mapping[row['spec_id']]] = []

                    if variants_info_details.has_key(row['product_id']) and variants_info_details[row['product_id']]['image']:
                        new_univ_arr[variant_specid_val_mapping[row['spec_id']]].append({row['spec_display_value']: variants_info_details[row['product_id']]['image']})
                    else:
                        new_univ_arr[variant_specid_val_mapping[row['spec_id']]].append({row['spec_display_value']: ""})

            for row in variants_universal:
                if row['spec_id'] != '2636':
                    if not new_univ_arr.has_key(variant_specid_val_mapping[row['spec_id']]):
                        new_univ_arr[variant_specid_val_mapping[row['spec_id']]] = []

                    new_univ_arr[variant_specid_val_mapping[row['spec_id']]].append({row['spec_display_value']: ""})



            universal_array = []

            for row in new_univ_arr:
                universal_array.append({row: new_univ_arr[row]})


            # product_spec_list[pid]['variants'].update(update(product_spec_list[pid]['variants'], {'details': {'universal': universal_array}}))
            if pid in variants_details['variants_info_details']:
                product_spec_list[pid]['variants'].update(update(product_spec_list[pid]['variants'], {'details': {'universal': universal_array}}))


    return product_spec_list


# Function for building product variant data
def build_variant_data(cursor, productIdsFromDb, product_variant_ids_from_db, variants, spec_display_array):
    # print("productIdsFromDb:",productIdsFromDb)
    # print("product_variant_ids_from_db:",product_variant_ids_from_db)
    # print("variants:",variants)
    # print("spec_display_array:",spec_display_array)
    # sys.exit()

    variants_pids = []
    for pid in productIdsFromDb:
        variants_pids.append(pid)
        words = product_variant_ids_from_db[pid].split(",")

        words = filter(None, words)
        # print('words:',words)

        if len(words):
            # print('words is not blank:',words)
            variants_pids = variants_pids + words

    # print("variants_pids:",variants_pids)
    # sys.exit()

    variants_pids_chunks = [variants_pids[x:x + LIMIT_PER_VARIANT_BATCH] for x in
                            xrange(0, len(variants_pids), LIMIT_PER_VARIANT_BATCH)]
    # print('variants_pids_chunks:', variants_pids_chunks)
    # print('length:',len(variants_pids_chunks))
    # sys.exit()

    variant_univ_specid = []
    for pid in productIdsFromDb:

        for row in variants:
            if spec_display_array.has_key(pid) and spec_display_array.get(pid).has_key(
                    row['spec_id']) and spec_display_array.get(pid).get(row['spec_id']).get('value'):
                variant_univ_specid.append(row['spec_id'])

    # print("variant_univ_specid:",variant_univ_specid)

    variants_universal_specs = ",".join(variant_univ_specid)
    variant_prods = product_variant_ids_from_db[pid]

    # print('variants_universal_specs:',variants_universal_specs)
    # sys.exit()

    response = {}

    if variants_universal_specs:
        variants_info_details = {}
        spec_indexes = {}

        for variant_list in variants_pids_chunks:

            variants_pids_str = ','.join(variant_list)

            sql = "SELECT product_displayname,a.product_id,jd_price,national_catid,images_crawled_path, product_imagepath FROM tbl_product_master a " \
                  "JOIN (SELECT tbl_product_image.product_id,tbl_product_image.product_imagepath FROM tbl_product_image WHERE product_id " \
                  "IN (%s) AND image_type=1 AND active_flag=1) t ON a.product_id = t.product_id WHERE a.product_id IN (%s) AND a.p_variants != '' " % (
                  variants_pids_str, variants_pids_str)

            # print("sql:",sql)
            # sys.exit()

            cursor.execute(sql)
            img_details = cursor.fetchall()

            for row in img_details:
                variants_info_details[str(row['product_id'])] = {'pdisp': row['product_displayname'],
                                                                 'jdguarantee_price': row['jd_price'],
                                                                 'national_catid': row['national_catid'],
                                                                 'product_id': row['product_id'],
                                                                 'image': row['product_imagepath']}

            # print("variants_info_details:",variants_info_details)
            # sys.exit()

            sql = "SELECT product_id, group_concat(concat(concat(spec_display_value,'|!|'),spec_id) ORDER BY field (spec_id,%s) " \
                  "SEPARATOR '|~|') AS spec_combined FROM tbl_product_spec_display WHERE product_id IN(%s) " \
                  "AND spec_id IN (%s) GROUP BY product_id " % (
                  variants_universal_specs, variants_pids_str, variants_universal_specs)

            cursor.execute(sql)
            variants_specs = cursor.fetchall()

            for row in variants_specs:
                # spec_indexes[row['spec_combined']] = row['product_id']
                temp_data = {}
                temp_data[row['spec_combined']] = row['product_id']
                spec_indexes[str(row['product_id'])] = temp_data

        response['variants_info_details'] = variants_info_details
        response['spec_indexes'] = spec_indexes

    return response


def update(d, u):
    for k, v in u.iteritems():
        if isinstance(d, collections.Mapping):
            if isinstance(v, collections.Mapping):
                r = update(d.get(k, {}), v)
                d[k] = r
            else:
                d[k] = u[k]
        else:
            d = {k: u[k]}
    return d


def my_key(dict_key):
    try:
        return int(dict_key)
    except ValueError:
        return dict_key


# Function for getting images of products
def get_images(cursor, productIdsFromDb):
    sql = "SELECT product_id, group_concat(product_imagepath ORDER BY image_type ASC, product_imagepath ASC separator '|~|') AS image_path " \
          "FROM tbl_product_image WHERE product_id IN (%s) AND active_flag=1 GROUP BY product_id " % (
              ','.join(productIdsFromDb))

    cursor.execute(sql)
    temp_product_images = cursor.fetchall()

    product_images = {}
    convert_es_format(temp_product_images)

    for row in temp_product_images:
        product_images[row['product_id']] = row['image_path']

    return product_images


# Function for processing new products
def new_products(catid, order_by):
    connection = get_connection()
    cursor = connection.cursor()
    catid = int(catid)
    es = Elasticsearch(NODES, timeout=60, max_retries=10, retry_on_timeout=True)
    
    if order_by:
        sql = "SELECT id, a.product_id, catid AS immediate_parent_catid,GROUP_CONCAT(DISTINCT process_flag) AS flag,IFNULL(b.in_stock,1) AS in_stock FROM %s a LEFT JOIN tbl_product_instock_info b ON a.product_id=b.product_id WHERE catid=%d AND a.product_id<>0 AND process_flag IN (0,2,9) GROUP BY product_id HAVING (flag ='0' OR flag='0,2' OR flag='0,9' OR flag='0,2,9' OR flag='0,9,2') ORDER BY id %s LIMIT %d" % (TABLENAME, catid, order_by, LIMIT_PER_THREAD_ON_NEW)

    else:
        sql = "SELECT id, a.product_id, catid AS immediate_parent_catid,GROUP_CONCAT(DISTINCT process_flag) AS flag,IFNULL(b.in_stock,1) AS in_stock FROM %s a LEFT JOIN tbl_product_instock_info b ON a.product_id=b.product_id WHERE catid=%d AND a.product_id<>0 AND process_flag IN (0,2,9) GROUP BY product_id HAVING (flag ='0' OR flag='0,2' OR flag='0,9' OR flag='0,2,9' OR flag='0,9,2') LIMIT %d" % (TABLENAME, catid, LIMIT_PER_THREAD_ON_NEW)

    cursor.execute(sql)
    product_ids_result = cursor.fetchall()
    product_ids_only = map(lambda x: x['product_id'], product_ids_result)

    product_ids_indexes = {}
    products_instock_info = {}
    for row in product_ids_result:
        product_ids_indexes[str(row['product_id'])] = str(row['id'])
        products_instock_info[str(row['product_id'])] = str(row['in_stock'])

    # sql = "SELECT a.catid, a.spec_id, b.spec_display_name, b.spec_unit, a.preview_spec_position, a.preview_active_flag, a.sort_active_flag, b.spec_unit_pos " \
    #       "FROM tbl_spec_mapping a JOIN tbl_spec_master b ON a.spec_id=b.spec_id WHERE a.catid=%d AND a.display_active_flag=1 AND b.active_flag=1 " \
    #       "ORDER BY a.display_spec_position ASC" % (catid)
    sql = "SELECT a.catid, a.spec_id, b.spec_display_name, b.spec_unit, a.preview_spec_position, a.preview_active_flag, a.sort_active_flag, b.spec_unit_pos FROM tbl_spec_mapping a JOIN tbl_spec_master b ON a.spec_id=b.spec_id WHERE a.catid=%d AND a.display_active_flag=1 AND b.active_flag=1 ORDER BY IF(a.spec_id=103,1,IF(a.spec_id=122,1,a.display_spec_position)) ASC" % (catid)
    cursor.execute(sql)
    cat_specs = cursor.fetchall()
    convert_es_format(cat_specs)

    sql = "SELECT DISTINCT spec_id,spec_name,spec_display_name FROM tbl_cat_variant WHERE catid=%d AND active_flag=1 ORDER BY id LIMIT 2" % (
        catid)
    cursor.execute(sql)
    variants = cursor.fetchall()
    convert_es_format(variants)

    products_list = []
    if product_ids_only:
        if ENV == 'production':
            sql = "UPDATE %s SET process_flag = 2 WHERE process_flag = 0 AND id IN (%s)" % (
            TABLENAME, ','.join(product_ids_indexes.values()))
            cursor.execute(sql)

        sql = "SELECT product_displayname AS pdname, product_price AS pprice, parent_lineage as plin, parentid_lineage as pidlin, " \
              " avg_quotes as avgqt, product_desc as pdesc, " \
              " preorder_flag as porder, p_variants, micro_site_flag as msflag, " \
              "IF(popularity_comp=9999,1,0) as pcomp, IF(popularity_comp=9999, product_popularity, 999999999) as ppop, jd_price AS jdprice, " \
              "lowestcomp_price AS lcompprice, barcode AS bcode, " \
              "exclusive_flag AS eflag, num_aff_link AS nalink, IF(aff_sales_rank!=0.00,aff_sales_rank,999999999999) as asrank, IF(images_crawled_path !='',1,0) " \
              "AS noimg , LCASE(IFNULL(images_crawled_path,'')) AS pimg, images_crawled_path as imgcpath, product_id AS pid, product_name as pname," \
              " catid as qqcid, catname as qqcname, " \
              "national_catid as ncid, product_brand as bname, product_brandid as bid, active_flag as aflag, source_flag as sflag FROM tbl_product_master  " \
              "WHERE catid=%d AND product_id IN (%s)" % (catid, ','.join(map(str, product_ids_only)))

        cursor.execute(sql)
        products_list = cursor.fetchall()

    while products_list:
        convert_es_format(products_list)
        product_ids_from_db = map(lambda x: x['pid'], products_list)

        product_variant_ids_from_db = {}
        for row in products_list:
            product_variant_ids_from_db[row['pid']] = row['p_variants']

        product_images = get_images(cursor, product_ids_from_db)
        product_specs = get_specs(cursor, catid, product_ids_from_db, cat_specs, variants, product_variant_ids_from_db)

        actions = []
        bulk_data = []
        popular_products_keyword = []
        unpopular_products_keyword = []
        for row in products_list:

            cur_datetime = time.strftime("%Y-%m-%d %H:%M:%S")
            ppid = product_ids_indexes[row['pid']]
            instore_flag = products_instock_info[row['pid']]

            row.update({"pavid": "", "minqt": "", "maxqt": "", "pqt": "", "pqtr": "", "estdur": "", "delchar":"", "jdgtag": "", "btntype": "get_quotes", "buyflag": '1', "variants": [], 'p_spec': {'d_spec': [], 'f_spec': []}, 'datetime':cur_datetime, 'ppid':ppid, 'ppscript':PROCESSING_SCRIPT_NAME, 'instore':instore_flag })

            del row['p_variants']

            if row['pid'] in product_specs:
                if product_specs[row['pid']].has_key('d_spec'):
                    row['p_spec']['d_spec'] = product_specs[row['pid']]['d_spec']
                if product_specs[row['pid']].has_key('f_spec'):
                    row['p_spec']['f_spec'] = product_specs[row['pid']]['f_spec']
                if product_specs[row['pid']].has_key('dval'):
                    row['dval'] = product_specs[row['pid']]['dval']
                if product_specs[row['pid']].has_key('pval'):
                    row['pval'] = product_specs[row['pid']]['pval']
                if product_specs[row['pid']].has_key('variants') and product_specs[row['pid']]['variants']:
                    row['variants'] = product_specs[row['pid']]['variants']
                if product_specs[row['pid']].has_key('pavid'):
                    row['pavid'] = product_specs[row['pid']]['pavid']

            if row['pid'] in product_images:
                if product_images[row['pid']]:
                    row['pimg'] = product_images[row['pid']]
                    row['noimg'] = '1'

            # Building Popular/Unpopular products list
            product_keyword_doc = {}
            product_keyword_doc['datetime'] = cur_datetime
            product_keyword_doc['ppscript'] = PROCESSING_SCRIPT_NAME
            product_keyword_doc['pid'] = row['pid']
            product_keyword_doc['pdname'] = row['pdname']
            product_keyword_doc['qqcname'] = row['qqcname']
            product_keyword_doc['bname'] = row['bname']
            product_keyword_doc['lcompprice'] = row['lcompprice']
            product_keyword_doc['jdprice'] = row['jdprice']
            product_keyword_doc['pprice'] = row['pprice']
            product_keyword_doc['keyword'] = ''

            if (int(row['qqcid']) in SF_POP_CATIDS):          
                popular_products_keyword.append(product_keyword_doc)            
            else:
                unpopular_products_keyword.append(product_keyword_doc)

            bulk_data.append({
                "index": {
                    '_index': ES_INDEX,
                    '_type': ES_TYPE,
                    '_id': row['pid']
                }
            })

            bulk_data.append(row)
            temp_es_limit = ES_LIMIT_PER_REQUEST * 2

            if len(bulk_data) == temp_es_limit:
                convert_es_format(bulk_data)
                es = Elasticsearch(NODES, timeout=60, max_retries=10, retry_on_timeout=True)
                responses = es.bulk(index=ES_INDEX, body=bulk_data, refresh=True)

                success = []
                errors = []
                for indexed in responses['items']:
                    if indexed.has_key('index'):
                        if indexed['index'].has_key('error'):
                            errors.append({'pid': indexed['index']['_id'], 'error': indexed['index']['error']})
                        elif product_ids_indexes.has_key(indexed['index']['_id']) and indexed['index']['status'] in [200, 201]:
                            success.append(product_ids_indexes[indexed['index']['_id']])

                if success and ENV == 'production':
                    sql = "UPDATE %s SET process_flag = 1 WHERE process_flag = 2 AND id IN (%s)" % (
                    TABLENAME, ','.join(success))
                    cursor.execute(sql)

                bulk_data = []

                # Process all popular/unpopular product ids with their respective keywords
                if len(popular_products_keyword) > 0:
                    keyword_index = ES_SF_KEYWORD['popular']['index']            
                    keyword_type = ES_SF_KEYWORD['popular']['type'] 
                    process_products_keyword(cursor, popular_products_keyword, keyword_index, keyword_type)
                    popular_products_keyword = []

                if len(unpopular_products_keyword) > 0:
                    keyword_index = ES_SF_KEYWORD['unpopular']['index']            
                    keyword_type = ES_SF_KEYWORD['unpopular']['type'] 
                    process_products_keyword(cursor, unpopular_products_keyword, keyword_index, keyword_type)                 
                    unpopular_products_keyword = []

        if len(bulk_data) > 0:
            convert_es_format(bulk_data)
            es = Elasticsearch(NODES, timeout=60, max_retries=10, retry_on_timeout=True)
            responses = es.bulk(index=ES_INDEX, body=bulk_data, refresh=True)

            success = []
            errors = []
            for indexed in responses['items']:
                if indexed.has_key('index'):
                    if indexed['index'].has_key('error'):
                        errors.append({'pid': indexed['index']['_id'], 'error': indexed['index']['error']})
                    elif product_ids_indexes.has_key(indexed['index']['_id']) and indexed['index']['status'] in [200, 201]:
                        success.append(product_ids_indexes[indexed['index']['_id']])

            if success and ENV == 'production':
                sql = "UPDATE %s SET process_flag = 1 WHERE process_flag = 2 AND id IN (%s)" % (
                TABLENAME, ','.join(success))
                cursor.execute(sql)

        # Process all popular/unpopular product ids with their respective keywords
        if len(popular_products_keyword) > 0:
            keyword_index = ES_SF_KEYWORD['popular']['index']            
            keyword_type = ES_SF_KEYWORD['popular']['type'] 
            process_products_keyword(cursor, popular_products_keyword, keyword_index, keyword_type)

        if len(unpopular_products_keyword) > 0:
            keyword_index = ES_SF_KEYWORD['unpopular']['index']            
            keyword_type = ES_SF_KEYWORD['unpopular']['type'] 
            process_products_keyword(cursor, unpopular_products_keyword, keyword_index, keyword_type)

        if order_by:
            sql = "SELECT id, a.product_id, catid AS immediate_parent_catid,GROUP_CONCAT(DISTINCT process_flag) AS flag,IFNULL(b.in_stock,1) AS in_stock FROM %s a LEFT JOIN tbl_product_instock_info b ON a.product_id=b.product_id WHERE catid=%d AND a.product_id<>0 AND process_flag IN (0,2,9) GROUP BY product_id HAVING (flag ='0' OR flag='0,2' OR flag='0,9' OR flag='0,2,9' OR flag='0,9,2') ORDER BY id %s LIMIT %d" % (TABLENAME, catid, order_by, LIMIT_PER_THREAD_ON_NEW)

        else:
            sql = "SELECT id, a.product_id, catid AS immediate_parent_catid,GROUP_CONCAT(DISTINCT process_flag) AS flag,IFNULL(b.in_stock,1) AS in_stock FROM %s a LEFT JOIN tbl_product_instock_info b ON a.product_id=b.product_id WHERE catid=%d AND a.product_id<>0 AND process_flag IN (0,2,9) GROUP BY product_id HAVING (flag ='0' OR flag='0,2' OR flag='0,9' OR flag='0,2,9' OR flag='0,9,2') LIMIT %d" % (TABLENAME, catid, LIMIT_PER_THREAD_ON_NEW)

        cursor.execute(sql)
        new_product_ids_result = cursor.fetchall()
        new_product_ids_only = map(lambda x: x['product_id'], new_product_ids_result)

        product_ids_indexes = {}
        products_instock_info = {}
        for row in new_product_ids_result:
            product_ids_indexes[str(row['product_id'])] = str(row['id'])
            products_instock_info[str(row['product_id'])] = str(row['in_stock'])

        if set(product_ids_only) == set(new_product_ids_only):
            print catid, "new products are same"
            break
        else:
            product_ids_only = new_product_ids_only

        if new_product_ids_only:
            if ENV == 'production':
                sql = "UPDATE %s SET process_flag = 2 WHERE process_flag = 0 AND id IN (%s)" % (TABLENAME, ','.join(product_ids_indexes.values()))
                cursor.execute(sql)

            sql = "SELECT product_displayname AS pdname, product_price AS pprice, parent_lineage as plin, parentid_lineage as pidlin, " \
                  " avg_quotes as avgqt, product_desc as pdesc, " \
                  " preorder_flag as porder, p_variants, micro_site_flag as msflag, " \
                  "IF(popularity_comp=9999,1,0) as pcomp, IF(popularity_comp=9999, product_popularity, 999999999) as ppop, jd_price AS jdprice, " \
                  "lowestcomp_price AS lcompprice, barcode AS bcode, " \
                  "exclusive_flag AS eflag, num_aff_link AS nalink, IF(aff_sales_rank!=0.00,aff_sales_rank,999999999999) as asrank, IF(images_crawled_path !='',1,0) " \
                  "AS noimg , LCASE(IFNULL(images_crawled_path,'')) AS pimg, images_crawled_path as imgcpath, product_id AS pid, product_name as pname," \
                  " catid as qqcid, catname as qqcname, " \
                  "national_catid as ncid, product_brand as bname, product_brandid as bid, active_flag as aflag, source_flag as sflag FROM tbl_product_master  " \
                  "WHERE catid=%d AND product_id IN (%s)" % (catid, ','.join(map(str, new_product_ids_only)))

            cursor.execute(sql)
            products_list = cursor.fetchall()
        else:
            products_list = []

    connection.close()


class MainPendingProducts(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        connection = get_connection()
        cursor = connection.cursor()
        sql = "SELECT catid, COUNT(1) AS cnt FROM %s WHERE product_id<>0 AND catid<>0 AND process_flag IN (2) GROUP BY catid ORDER BY " \
              "FIELD(catid, 1000061,1000055,1000046,1000045,1000040,1000038,1000022,1000017,1000013,1000012) DESC, cnt DESC" % (
                  TABLENAME)
        cursor.execute(sql)
        results = cursor.fetchall()

        for row in results:
            catid = int(row['catid'])

            sql = "SELECT id, a.product_id, catid AS immediate_parent_catid,GROUP_CONCAT(DISTINCT process_flag) AS flag,IFNULL(b.in_stock,1) AS in_stock FROM %s a LEFT JOIN tbl_product_instock_info b ON a.product_id=b.product_id WHERE catid=%d AND a.product_id<>0 AND process_flag IN (0,2,9) GROUP BY product_id HAVING (flag ='2' OR flag='2,0' OR flag='2,9' OR flag='2,0,9' OR flag='2,9,0') LIMIT %d" % (TABLENAME, catid, LIMIT_PER_THREAD_ON_NEW)

            cursor.execute(sql)
            product_ids_result = cursor.fetchall()
            
            product_ids_only = map(lambda x: x['product_id'], product_ids_result)

            product_ids_indexes = {}
            products_instock_info = {}
            for row in product_ids_result:
                product_ids_indexes[str(row['product_id'])] = str(row['id'])
                products_instock_info[str(row['product_id'])] = str(row['in_stock'])

            # sql = "SELECT a.catid, a.spec_id, b.spec_display_name, b.spec_unit, a.preview_spec_position, a.preview_active_flag, a.sort_active_flag, b.spec_unit_pos " \
            #       "FROM tbl_spec_mapping a JOIN tbl_spec_master b ON a.spec_id=b.spec_id WHERE a.catid=%d AND a.display_active_flag=1 AND b.active_flag=1 " \
            #       "ORDER BY a.display_spec_position ASC" % (catid)
            sql = "SELECT a.catid, a.spec_id, b.spec_display_name, b.spec_unit, a.preview_spec_position, a.preview_active_flag, a.sort_active_flag, b.spec_unit_pos FROM tbl_spec_mapping a JOIN tbl_spec_master b ON a.spec_id=b.spec_id WHERE a.catid=%d AND a.display_active_flag=1 AND b.active_flag=1 ORDER BY IF(a.spec_id=103,1,IF(a.spec_id=122,1,a.display_spec_position)) ASC" % (catid)
            cursor.execute(sql)
            cat_specs = cursor.fetchall()
            convert_es_format(cat_specs)

            sql = "SELECT DISTINCT spec_id,spec_name,spec_display_name FROM tbl_cat_variant WHERE catid=%d AND active_flag=1 ORDER BY id LIMIT 2" % (catid)
            cursor.execute(sql)
            variants = cursor.fetchall()
            convert_es_format(variants)

            products_list = []
            if product_ids_only:
                sql = "SELECT product_displayname AS pdname, product_price AS pprice, parent_lineage as plin, parentid_lineage as pidlin, " \
                      " avg_quotes as avgqt, product_desc as pdesc, " \
                      " preorder_flag as porder, p_variants, micro_site_flag as msflag, " \
                      "IF(popularity_comp=9999,1,0) as pcomp, IF(popularity_comp=9999, product_popularity, 999999999) as ppop, jd_price AS jdprice, " \
                      "lowestcomp_price AS lcompprice, barcode AS bcode, " \
                      "exclusive_flag AS eflag, num_aff_link AS nalink, IF(aff_sales_rank!=0.00,aff_sales_rank,999999999999) as asrank, IF(images_crawled_path !='',1,0) " \
                      "AS noimg , LCASE(IFNULL(images_crawled_path,'')) AS pimg, images_crawled_path as imgcpath, product_id AS pid, product_name as pname," \
                      " catid as qqcid, catname as qqcname, " \
                      "national_catid as ncid, product_brand as bname, product_brandid as bid, active_flag as aflag, source_flag as sflag FROM tbl_product_master  " \
                      "WHERE catid=%d AND product_id IN (%s)" % (catid, ','.join(map(str, product_ids_only)))

                cursor.execute(sql)
                products_list = cursor.fetchall()

            while products_list:
                convert_es_format(products_list)
                product_ids_from_db = map(lambda x: x['pid'], products_list)

                product_variant_ids_from_db = {}
                for row in products_list:
                    product_variant_ids_from_db[row['pid']] = row['p_variants']

                product_images = get_images(cursor, product_ids_from_db)
                product_specs = get_specs(cursor, catid, product_ids_from_db, cat_specs, variants, product_variant_ids_from_db)

                bulk_data = []
                popular_products_keyword = []
                unpopular_products_keyword = []
                for row in products_list:

                    cur_datetime = time.strftime("%Y-%m-%d %H:%M:%S")                    
                    ppid = product_ids_indexes[row['pid']]
                    instore_flag = products_instock_info[row['pid']]

                    row.update(
                        {"pavid": "", "minqt": "", "maxqt": "", "pqt": "", "pqtr": "", "estdur": "", "delchar": "",
                         "jdgtag": "", "btntype": "get_quotes", "buyflag": '1', "variants": [],
                         'p_spec': {'d_spec': [], 'f_spec': []}, 'datetime':cur_datetime, 'ppid':ppid, 'ppscript':PROCESSING_SCRIPT_NAME, 'instore':instore_flag })

                    del row['p_variants']

                    if row['pid'] in product_specs:
                        if product_specs[row['pid']].has_key('d_spec'):
                            row['p_spec']['d_spec'] = product_specs[row['pid']]['d_spec']
                        if product_specs[row['pid']].has_key('f_spec'):
                            row['p_spec']['f_spec'] = product_specs[row['pid']]['f_spec']
                        if product_specs[row['pid']].has_key('dval'):
                            row['dval'] = product_specs[row['pid']]['dval']
                        if product_specs[row['pid']].has_key('pval'):
                            row['pval'] = product_specs[row['pid']]['pval']
                        if product_specs[row['pid']].has_key('variants') and product_specs[row['pid']]['variants']:
                            row['variants'] = product_specs[row['pid']]['variants']
                        if product_specs[row['pid']].has_key('pavid'):
                            row['pavid'] = product_specs[row['pid']]['pavid']

                    if row['pid'] in product_images:
                        if product_images[row['pid']]:
                            row['pimg'] = product_images[row['pid']]
                            row['noimg'] = '1'

                    # Building Popular/Unpopular products list
                    product_keyword_doc = {}
                    product_keyword_doc['datetime'] = cur_datetime
                    product_keyword_doc['ppscript'] = PROCESSING_SCRIPT_NAME
                    product_keyword_doc['pid'] = row['pid']
                    product_keyword_doc['pdname'] = row['pdname']
                    product_keyword_doc['qqcname'] = row['qqcname']
                    product_keyword_doc['bname'] = row['bname']
                    product_keyword_doc['lcompprice'] = row['lcompprice']
                    product_keyword_doc['jdprice'] = row['jdprice']
                    product_keyword_doc['pprice'] = row['pprice']
                    product_keyword_doc['keyword'] = ''

                    if (int(row['qqcid']) in SF_POP_CATIDS):          
                        popular_products_keyword.append(product_keyword_doc)            
                    else:
                        unpopular_products_keyword.append(product_keyword_doc)

                    bulk_data.append({
                        "index": {
                            '_index': ES_INDEX,
                            '_type': ES_TYPE,
                            '_id': row['pid']
                        }
                    })

                    bulk_data.append(row)

                    if len(bulk_data) / 2 == ES_LIMIT_PER_REQUEST:
                        convert_es_format(bulk_data)
                        es = Elasticsearch(NODES, timeout=60, max_retries=10, retry_on_timeout=True)
                        responses = es.bulk(index=ES_INDEX, body=bulk_data, refresh=True)

                        success = []
                        errors = []
                        for indexed in responses['items']:
                            if indexed.has_key('index'):
                                if indexed['index'].has_key('error'):
                                    errors.append(
                                        {'pid': indexed['index']['_id'], 'error': indexed['index']['error']})
                                elif product_ids_indexes.has_key(indexed['index']['_id']) and indexed['index'][
                                'status'] in [200, 201]:
                                    success.append(product_ids_indexes[indexed['index']['_id']])

                        if success and ENV == 'production':
                            sql = "UPDATE %s SET process_flag = 1 WHERE process_flag = 2 AND id IN (%s)" % (
                                TABLENAME, ','.join(success))
                            cursor.execute(sql)

                        bulk_data = []

                        # Process all popular/unpopular product ids with their respective keywords
                        if len(popular_products_keyword) > 0:                        
                            keyword_index = ES_SF_KEYWORD['popular']['index']            
                            keyword_type = ES_SF_KEYWORD['popular']['type'] 
                            process_products_keyword(cursor, popular_products_keyword, keyword_index, keyword_type)
                            popular_products_keyword = []

                        if len(unpopular_products_keyword) > 0:
                            keyword_index = ES_SF_KEYWORD['unpopular']['index']            
                            keyword_type = ES_SF_KEYWORD['unpopular']['type'] 
                            process_products_keyword(cursor, unpopular_products_keyword, keyword_index, keyword_type)                 
                            unpopular_products_keyword = []

                if len(bulk_data) > 0:
                    convert_es_format(bulk_data)
                    es = Elasticsearch(NODES, timeout=60, max_retries=10, retry_on_timeout=True)
                    responses = es.bulk(index=ES_INDEX, body=bulk_data, refresh=True)

                    success = []
                    errors = []
                    for indexed in responses['items']:
                        if indexed.has_key('index'):
                            if indexed['index'].has_key('error'):
                                errors.append({'pid': indexed['index']['_id'], 'error': indexed['index']['error']})
                            elif product_ids_indexes.has_key(indexed['index']['_id']) and indexed['index'][
                                'status'] in [200, 201]:
                                success.append(product_ids_indexes[indexed['index']['_id']])

                    if success and ENV == 'production':
                        sql = "UPDATE %s SET process_flag = 1 WHERE process_flag = 2 AND id IN (%s)" % (
                            TABLENAME, ','.join(success))
                        cursor.execute(sql)

                # Process all popular/unpopular product ids with their respective keywords
                if len(popular_products_keyword) > 0:
                    keyword_index = ES_SF_KEYWORD['popular']['index']            
                    keyword_type = ES_SF_KEYWORD['popular']['type'] 
                    process_products_keyword(cursor, popular_products_keyword, keyword_index, keyword_type)

                if len(unpopular_products_keyword) > 0:
                    keyword_index = ES_SF_KEYWORD['unpopular']['index']            
                    keyword_type = ES_SF_KEYWORD['unpopular']['type'] 
                    process_products_keyword(cursor, unpopular_products_keyword, keyword_index, keyword_type)

                sql = "SELECT id, a.product_id, catid AS immediate_parent_catid, GROUP_CONCAT(DISTINCT process_flag) AS flag,IFNULL(b.in_stock,1) AS in_stock FROM %s a LEFT JOIN tbl_product_instock_info b ON a.product_id=b.product_id WHERE catid=%d AND a.product_id<>0 AND process_flag IN (0,2,9) GROUP BY product_id HAVING (flag ='2' OR flag='2,0' OR flag='2,9' OR flag='2,0,9' OR flag='2,9,0') LIMIT %d" % (TABLENAME, catid, LIMIT_PER_THREAD_ON_NEW)                    
                cursor.execute(sql)
                new_product_ids_result = cursor.fetchall()
                new_product_ids_only = map(lambda x: x['product_id'], new_product_ids_result)

                # Building products instock list
                products_instock_info = {}
                for row in new_product_ids_result:
                    products_instock_info[str(row['product_id'])] = str(row['in_stock'])
                    
                if set(product_ids_only) == set(new_product_ids_only):
                    print catid, "pending products are same"
                    break
                else:
                    product_ids_only = new_product_ids_only
                    
                if new_product_ids_only:
                    sql = "SELECT product_displayname AS pdname, product_price AS pprice, parent_lineage as plin, parentid_lineage as pidlin, " \
                          " avg_quotes as avgqt, product_desc as pdesc, " \
                          " preorder_flag as porder, p_variants, micro_site_flag as msflag, " \
                          "IF(popularity_comp=9999,1,0) as pcomp, IF(popularity_comp=9999, product_popularity, 999999999) as ppop, jd_price AS jdprice, " \
                          "lowestcomp_price AS lcompprice, barcode AS bcode, " \
                          "exclusive_flag AS eflag, num_aff_link AS nalink, IF(aff_sales_rank!=0.00,aff_sales_rank,999999999999) as asrank, IF(images_crawled_path !='',1,0) " \
                          "AS noimg , LCASE(IFNULL(images_crawled_path,'')) AS pimg, images_crawled_path as imgcpath, product_id AS pid, product_name as pname," \
                          " catid as qqcid, catname as qqcname, " \
                          "national_catid as ncid, product_brand as bname, product_brandid as bid, active_flag as aflag, source_flag as sflag FROM tbl_product_master  " \
                          "WHERE catid=%d AND product_id IN (%s)" % (catid, ','.join(map(str, new_product_ids_only)))

                    cursor.execute(sql)
                    products_list = cursor.fetchall()
                else:
                    products_list = []

        if ENV == 'production':
            sql = "SHOW SLAVE STATUS"
            cursor.execute(sql)
            slave_info = cursor.fetchone()

            if slave_info and slave_info['Slave_IO_Running'] == "Yes" and slave_info['Slave_SQL_Running'] == 'Yes' and \
                    slave_info['Seconds_Behind_Master'] == 0:
                cursor.execute(sql)
                slave_info = cursor.fetchone()

                if slave_info and slave_info['Slave_IO_Running'] == "Yes" and slave_info[
                    'Slave_SQL_Running'] == 'Yes' and slave_info['Seconds_Behind_Master'] == 0:
                    sql = "DELETE FROM %s WHERE process_flag IN (2)" % (TABLENAME)
                    cursor.execute(sql)

        connection.close()


class MainDeleteProducts(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        connection = get_connection()
        cursor = connection.cursor()

        sql = "SELECT id, product_id, GROUP_CONCAT(DISTINCT process_flag) AS flag FROM %s" \
              " WHERE product_id<>0 AND process_flag IN (0,2,9) GROUP BY product_id HAVING (flag ='9' OR flag='9,0' OR flag='9,2' " \
              "OR flag='9,0,2' OR flag='9,2,0') LIMIT %d" % (TABLENAME, LIMIT_PER_THREAD_ON_DELETE)
        cursor.execute(sql)
        product_ids_result = cursor.fetchall()

        product_ids_only = map(lambda x: x['product_id'], product_ids_result)
        product_ids_indexes = {}
        for row in product_ids_result:
            product_ids_indexes[str(row['product_id'])] = str(row['id'])

        while product_ids_only:
            bulk_data = []
            keyword_pids = []

            for pid in product_ids_only:
                bulk_data.append({
                    "delete": {
                        '_index': ES_INDEX,
                        '_type': ES_TYPE,
                        '_id': pid
                    }
                })

                keyword_pids.append(pid)

                if len(bulk_data) == ES_LIMIT_PER_REQUEST:
                    es = Elasticsearch(NODES, timeout=60, max_retries=10, retry_on_timeout=True)
                    responses = es.bulk(index=ES_INDEX, body=bulk_data, refresh=True)

                    success = []
                    errors = []
                    for indexed in responses['items']:
                        if indexed.has_key('delete'):
                            if indexed['delete'].has_key('error'):
                                errors.append({'pid': indexed['delete']['_id'], 'error': indexed['delete']['error']})
                            elif product_ids_indexes.has_key(indexed['delete']['_id']) and indexed['delete'][
                                'status'] in [200, 201, 404]:
                                success.append(product_ids_indexes[indexed['delete']['_id']])

                    if success and ENV == 'production':
                        sql = "UPDATE %s SET process_flag = 8 WHERE process_flag IN (9) AND id IN (%s)" % (
                            TABLENAME, ','.join(success))
                        cursor.execute(sql)

                    bulk_data = []

                    # Deleting products ids from keyword(popular/unpopular) index                    
                    delete_keyword_pids(keyword_pids, ES_SF_KEYWORD['popular']['index'], ES_SF_KEYWORD['popular']['type'])
                    delete_keyword_pids(keyword_pids, ES_SF_KEYWORD['unpopular']['index'], ES_SF_KEYWORD['unpopular']['type'])
                    keyword_pids = []

            if len(bulk_data) > 0:
                es = Elasticsearch(NODES, timeout=60, max_retries=10, retry_on_timeout=True)
                responses = es.bulk(index=ES_INDEX, body=bulk_data, refresh=True)

                success = []
                errors = []
                for indexed in responses['items']:
                    if indexed.has_key('delete'):
                        if indexed['delete'].has_key('error'):
                            errors.append({'pid': indexed['delete']['_id'], 'error': indexed['delete']['error']})
                        elif product_ids_indexes.has_key(indexed['delete']['_id']) and indexed['delete']['status'] in [
                            200, 201, 404]:
                            success.append(product_ids_indexes[indexed['delete']['_id']])

                if success and ENV == 'production':
                    sql = "UPDATE %s SET process_flag = 8 WHERE process_flag IN (9) AND id IN (%s)" % (
                        TABLENAME, ','.join(success))
                    cursor.execute(sql)

            # Deleting products ids from keyword(popular/unpopular) index
            if len(keyword_pids) > 0:
                delete_keyword_pids(keyword_pids, ES_SF_KEYWORD['popular']['index'], ES_SF_KEYWORD['popular']['type'])
                delete_keyword_pids(keyword_pids, ES_SF_KEYWORD['unpopular']['index'], ES_SF_KEYWORD['unpopular']['type'])

            sql = "SELECT id, product_id, GROUP_CONCAT(DISTINCT process_flag) AS flag FROM %s" \
                  " WHERE product_id<>0 AND process_flag IN (0,2,9) GROUP BY product_id HAVING (flag ='9' OR flag='9,0' OR flag='9,2' OR flag='9,0,2' " \
                  "OR flag='9,2,0') LIMIT %d" % (TABLENAME, LIMIT_PER_THREAD_ON_DELETE)
            cursor.execute(sql)
            new_product_ids_result = cursor.fetchall()
            new_product_ids_only = map(lambda x: x['product_id'], new_product_ids_result)

            if set(product_ids_only) == set(new_product_ids_only):
                print "delete products are same"
                break
            else:
                product_ids_only = new_product_ids_only

        connection.close()


class MainNewProducts(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        connection = get_connection()
        cursor = connection.cursor()
        sql = "SELECT catid, COUNT(1) AS cnt FROM %s WHERE product_id<>0 AND catid<>0 AND process_flag IN (0) " \
              "GROUP BY catid ORDER BY FIELD(catid, 1000061,1000055,1000046,1000045,1000040,1000038,1000022,1000017,1000013,1000012) DESC, cnt DESC" % (
                  TABLENAME)
        cursor.execute(sql)
        results = cursor.fetchall()
        connection.close()

        executor = ThreadPoolExecutor(max_workers=NEW_ACTIVE_THREADS)
        for row in results:
            if row['cnt'] < 20000:
                executor.submit(new_products, row['catid'], "")
            else:
                executor.submit(new_products, row['catid'], "ASC")
                executor.submit(new_products, row['catid'], "DESC")


# try to update pending products
main_pending_products = MainPendingProducts()
main_pending_products.start()

# delete products
delete_proudcs = MainDeleteProducts()
delete_proudcs.start()

# update products with modified or new data
newProducts = MainNewProducts()
newProducts.start()

# new_products(1000004, "")
