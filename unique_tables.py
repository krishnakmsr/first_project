#mysql -h 192.168.42.67 -u krishnak -p'kR!shn@k@#$#1'
import pymongo
from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
from collections import OrderedDict
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = pymongo.MongoClient('mongodb://192.168.13.65:27017')
db1=client['db_test']
#db1=client['db_marketplace_passbook']



x1=db1.list_collection_names()


vexpr_col6='tbl_mp_event_unique_data_20220502'
if(vexpr_col6 not in x1):
	db1.create_collection(vexpr_col6)
        vexpr6={
		"$jsonSchema" : {
			"bsonType" : "object",
			"required" : [
				"e_dt",
				"ssid",
				"di",
				"pid",
				"d_pid",
				"evt_flg"
			],
			"properties" : {
				"e_dt" : {
					"bsonType" : "date",
					"description" : "entry date - must be timestamp and is required"
				},
				"ssid" : {
					"bsonType" : "string",
					"description" : "session id - must be string in lower case  and is required"
				},
				"di" : {
					"bsonType" : "string",
					"description" : "docid - must be string  in upper case  and is not required"
				},
				"pid" : {
					"bsonType" : "string",
					"description" : "product id  - must be string  and is required"
				},
				"d_pid" : {
					"bsonType" : "string",
					"description" : "dc product id  - must be string and is required"
				},
				"evt_flg" : {
					"bsonType" : "int",
					"description" : "event flag - must be int and is required"
				},
				"sevt_flg" : {
					"bsonType" : "int",
					"description" : "sub event flag - must be int and is required"
				},
				"pt_flg" : {
					"bsonType" : "int",
					"description" : "page type flag - must be int and is required"
				},
				"dv_src" : {
					"bsonType" : "int",
					"description" : "device source - must be int and is required"
				},
				"pos" : {
					"bsonType" : "int",
					"description" : "position - must be int and is required"
				},
				"bn_flg" : {
					"bsonType" : "int",
					"description" : "banner flag - must be int and is not required"
				}
			}
		}
	}

	query6=OrderedDict()
	query6['collMod']='tbl_mp_event_unique_data_20220502'
	query6['validator']=vexpr6
	query6['validationLevel']='moderate'
	query6['validationAction']='warn' or 'error'


	db1.command(query6)
	collection_daily6='tbl_mp_event_unique_data_20220502'
 	colllectionObj_daily6=db1['tbl_mp_event_unique_data_20220502']  # collection obj

	index_daily6=[("e_dt", pymongo.ASCENDING),("ssid", pymongo.ASCENDING),("di", pymongo.ASCENDING),("pid", pymongo.ASCENDING),("d_pid", pymongo.ASCENDING),("evt_flg", pymongo.ASCENDING)]

	colllectionObj_daily6.create_index(index_daily6,unique=True) 


vexpr_col5='tbl_mp_event_data_20220502'
if(vexpr_col5 not in x1):
	db1.create_collection(vexpr_col5)
	vexpr5= {
		"$jsonSchema" : {
			"bsonType" : "object",
			"required" : [ "r_dt", "ssid", "srchid", "di", "pid" ],
			"properties" : {
				"r_dt" : {
					"bsonType" : "date",
					"description" : "entry date - must be timestamp and is required"
				},
				"ssid" : {
					"bsonType" : "string",
					"description" : "session id - must be string in lower case and is required"
				},
				"srchid" : {
					"bsonType" : "string",
					"description" : "search id - must be string in lower case and is required"
				},
				"di" : {
					"bsonType" : "string",
					"description" : "docid - must be string in upper case and is required"
				},
				"bc_di" : {
					"bsonType" : "string",
					"description" : "brand connect docid - must be string in upper case and is required"
				},
				"cm_nm" : {
					"bsonType" : "string",
					"description" : "companyname - must be string and is not required"
				},
				"s_flg" : {
					"bsonType" : "int",
					"description" : "sponsored flag - must be string and is not required"
				},
				"s_bid" : {
					"bsonType" : "decimal",
					"description" : "sponsored bid - must be decimal128 and is not required"
				},
				"bb_flg" : {
					"bsonType" : "int",
					"description" : "best buy flag - must be int and is not required"
				},
				"jdc_flg" : {
					"bsonType" : "int",
					"description" : "justdial choice flag - must be int and is not required"
				},
				"bbx_flg" : {
					"bsonType" : "int",
					"description" : "buy box flag - must be int and is not required"
				},
				"ty" : {
					"bsonType" : "string",
					"description" : "type - must be string in lower case and is not required"
				},
				"pid" : {
					"bsonType" : "string",
					"description" : "product id - must be string  and is not required"
				},
				"pnm" : {
					"bsonType" : "string",
					"description" : "product name - must be string and is not required"
				},
				"d_pid" : {
					"bsonType" : "string",
					"description" : "dc product id - must be string  and is not required"
				},
				"psvnid" : {
					"bsonType" : "string",
					"description" : "psv node id - must be string and is not required"
				},
				"psvnm" : {
					"bsonType" : "string",
					"description" : "psv node name - must be string in lower case and is not required"
				},
				"pc" : {
					"bsonType" : "int",
					"description" : "pincode - must be integer case and is not required"
				},
				"d_ct" : {
					"bsonType" : "string",
					"description" : "data city - must be string in lower case and is not required"
				},
				"ct" : {
					"bsonType" : "string",
					"description" : "city - must be string in lower case and is not required"
				},
				"dsrc" : {
					"bsonType" : "string",
					"description" : "data source - must be string in lower case and is not required"
				},
				"nc" : {
					"bsonType" : "int",
					"description" : "national catid - must be int and is not required"
				},
				"paid" : {
					"bsonType" : "int",
					"description" : "paid - must be int and is not required"
				},
				"rf" : {
					"bsonType" : "string",
					"description" : "referrer - must be string in lower and is not required"
				},
				"rf_flg" : {
					"bsonType" : "int",
					"description" : "referrer_flag - must be int and is not required"
				},
				"rfa" : {
					"bsonType" : "string",
					"description" : "referrer_attribute - must be string in lower case and is not required"
				},
				"rfa_flg" : {
					"bsonType" : "int",
					"description" : "referrer_attribute_flag - must be int and is not required"
				},
				"pt" : {
					"bsonType" : "string",
					"description" : "page type - must be string in lower case and is not required"
				},
				"pt_flg" : {
					"bsonType" : "int",
					"description" : "page type flag - must be int and is not required"
				},
				"evt" : {
					"bsonType" : "string",
					"description" : "event name - must be string in lower case and is not required"
				},
				"evt_flg" : {
					"bsonType" : "int",
					"description" : "event name flag - must be int and is not required"
				},
				"sevt" : {
					"bsonType" : "string",
					"description" : "sub event name - must be string in lower case and is not required"
				},
				"sevt_flg" : {
					"bsonType" : "int",
					"description" : "sub event name flag - must be int and is not required"
				},
				"uip" : {
					"bsonType" : "string",
					"description" : "user ip - must be string and is not required"
				},
				"ar" : {
					"bsonType" : "string",
					"description" : "areaname - must be string in lower case and is not required"
				},
				"jduid" : {
					"bsonType" : "string",
					"description" : "jduid - must be string and is not required"
				},
				"dv_src" : {
					"bsonType" : "int",
					"description" : "device source - must be int and is not required"
				},
				"dv_id" : {
					"bsonType" : "string",
					"description" : "device id - must be string and is not required"
				},
				"cat_nm" : {
					"bsonType" : "string",
					"description" : "category name - must be string in lower case and is not required"
				},
				"ua" : {
					"bsonType" : "string",
					"description" : "user agent - must be string and is not required"
				},
				"ci" : {
					"bsonType" : "int",
					"description" : "campaign id - must be int and is not required"
				},
				"pos" : {
					"bsonType" : "int",
					"description" : "position - must be int and is not required"
				},
				"d_flg" : {
					"bsonType" : "int",
					"description" : "deduction flag - must be int and is not required"
				},
				"ob" : {
					"bsonType" : "decimal",
					"description" : "opening balance - must be decimal128 and is not required"
				},
				"oc" : {
					"bsonType" : "decimal",
					"description" : "closing balance - must be decimal128 and is not required"
				},
				"ccb" : {
					"bsonType" : "object",
					"description" : "consolidated closing balance - must be object and is not required"
				},
				"ins" : {
					"bsonType" : "string",
					"description" : "instrument id - must be string and is not required"
				},
				"ord_id" : {
					"bsonType" : "string",
					"description" : "ordered id - must be string and is not required"
				},
				"txd" : {
					"bsonType" : "object",
					"description" : "txn details - must be object and is not required"
				},
				"ods_com" : {
					"bsonType" : "object",
					"description" : "ods_commission_info - must be object and is not required"
				},
				"ls" : {
					"bsonType" : "int",
					"description" : "lead_source - must be int and is not required"
				},
				"orq" : {
					"bsonType" : "int",
					"description" : "origin_req - must be int and is not required"
				},
				"st" : {
					"bsonType" : "int",
					"description" : "search_type - must be int and is not required"
				},
				"lt_cl" : {
					"bsonType" : "int",
					"description" : "late_call - must be int and is not required"
				},
				"th_id" : {
					"bsonType" : "int",
					"description" : "thread_id - must be int and is not required"
				},
				"mt_flg" : {
					"bsonType" : "int",
					"description" : "monetary_flag - must be int and is not required"
				},
				"gst_amt" : {
					"bsonType" : "decimal",
					"description" : "gst_amt - must be decimal128 and is not required"
				},
				"txn_amt" : {
					"bsonType" : "decimal",
					"description" : "transaction_amount - must be decimal128 and is not required"
				},
				"amt" : {
					"bsonType" : "decimal",
					"description" : "amt_deducted - must be decimal128 and is not required"
				},
				"n_amt" : {
					"bsonType" : "decimal",
					"description" : "net_amt_deducted - must be decimal128 and is not required"
				},
				"d_amt" : {
					"bsonType" : "decimal",
					"description" : "delta_amt_deducted - must be decimal128 and is not required"
				},
				"disc_amt" : {
					"bsonType" : "decimal",
					"description" : "discount_amt_deducted - must be decimal128 and is not required"
				},
				"bal" : {
					"bsonType" : "decimal",
					"description" : "balance - must be decimal128 and is not required"
				},
				"n_bal" : {
					"bsonType" : "decimal",
					"description" : "net_balance - must be decimal128 and is not required"
				},
				"d_bal" : {
					"bsonType" : "decimal",
					"description" : "delta_balance - must be decimal128 and is not required"
				},
				"disc_bal" : {
					"bsonType" : "decimal",
					"description" : "discount_balance - must be decimal128 and is not required"
				},
				"d_dt" : {
					"bsonType" : [ "date", "null" ],
					"description" : "deduction_date - must be timestamp and is not required"
				},
				"txn_dt" : {
					"bsonType" : [ "date", "null" ],
					"description" : "transaction_time - must be timestamp and is not required"
				},
				"dup_cal" : {
					"bsonType" : "int",
					"description" : "duplicate_caller - must be int and is not required"
				},
				"inv_ci" : {
					"bsonType" : "int",
					"description" : "charged_campaign_id - must be int and is not required"
				},
				"c_dsrc" : {
					"bsonType" : "string",
					"description" : "contract_datasrc - must be string in lower and is not required"
				},
				"cm_ty" : {
					"bsonType" : "string",
					"description" : "campaign_type - must be string in lower and is not required"
				},
				"ost" : {
					"bsonType" : "int",
					"description" : "old_structure - must be int and is not required"
				},
				"ps_flg" : {
					"bsonType" : "int",
					"description" : "ps_flg - must be int and is not required"
				},
				"is_rfd" : {
					"bsonType" : "int",
					"description" : "is_refunded - must be int and is not required"
				},
				"rfd_id" : {
					"bsonType" : "string",
					"description" : "refund_obj_id - must be string and is not required"
				},
				"on_rfd" : {
					"bsonType" : "string",
					"description" : "on_refund_obj_id - must be string and is not required"
				},
				"rf_dt" : {
					"bsonType" : [ "date", "null" ],
					"description" : "refund_date - must be timestamp and is not required"
				},
				"rv_dt" : {
					"bsonType" : [ "date", "null" ],
					"description" : "reversal_date - must be timestamp and is not required"
				},
				"ad_amt" : {
					"bsonType" : "decimal",
					"description" : "monetized_adjusted_amt - must be decimal128 and is not required"
				},
				"ad_n_amt" : {
					"bsonType" : "decimal",
					"description" : "monetized_adjusted_net_amt - must be decimal128 and is not required"
				},
				"ad_d_amt" : {
					"bsonType" : "decimal",
					"description" : "monetized_adjusted_delta_amt - must be decimal128 and is not required"
				},
				"ad_disc_amt" : {
					"bsonType" : "decimal",
					"description" : "monetized_adjusted_discount_amt - must be decimal128 and is not required"
				},
				"fdi" : {
					"bsonType" : "object",
					"description" : "finance_deduction_info - must be object and is not required"
				},
				"ctid" : {
					"bsonType" : "string",
					"description" : "ctid - must be string and is required"
				},
				"logic_id" : {
					"bsonType" : "string",
					"description" : "logic_id - must be string and is required"
				},
				"kw_id" : {
					"bsonType" : "string",
					"description" : "keyword_id - must be string and is required"
				},
				"kw_nm" : {
					"bsonType" : "string",
					"description" : "keyword_ name - must be string and is required"
				},
				"dc" : {
					"bsonType" : "string",
					"description" : "data_city - must be string and is required"
				},
				"bn_flg" : {
					"bsonType" : "int",
					"description" : "banner flag - must be int and is not required"
				}
			}
		}
	}
    
	query5=OrderedDict()
	query5['collMod']='tbl_mp_event_data_20220502'
	query5['validator']=vexpr5
	query5['validationLevel']='moderate'
	query5['validationAction']='warn' or 'error'


	db1.command(query5)

	collection_daily5='tbl_mp_event_data_20220502'
	colllectionObj_daily5=db1['tbl_mp_event_data_20220502']  # collection obj
	colllectionObj_daily5.create_index("bn_flg")
