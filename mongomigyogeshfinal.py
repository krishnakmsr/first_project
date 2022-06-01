
import mysql.connector
import pymongo
import json
import sys
from decimal import Decimal
from bson.decimal128 import Decimal128, create_decimal128_context


mydb = mysql.connector.connect(
  host="localhost",
  user="sandeepj",
  passwd="sandeepj!@#",
  database="test"
)


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb1 = myclient["test"]
mycol1 = mydb1["tbl_product_info_mongo"]

mycursor = mydb.cursor()
details=("SELECT product_id FROM tbl_master  ")

mycursor.execute(details)
Rec = mycursor.fetchall() 
for mast in Rec:
 	product_id=mast[0]	
	details='SELECT product_id,product_name,actual_product_name,product_displayname,product_searchname,product_searchname_WS,product_searchname_processed,product_searchname_ignore_processed,parent_lineage,parentid_lineage,product_brandid,product_brand,product_url,product_imageurl,product_price,CASE WHEN product_otherimages IS NULL THEN " " ELSE product_otherimages END AS product_otherimages, CASE WHEN  images_crawled_path IS NULL THEN "" ELSE images_crawled_path END AS images_crawled_path,brandcat_popularity,product_popularity,product_callcount,product_callcount_rolling,product_weight,national_catid,active_flag,autosuggest_flag, CASE WHEN  createdby IS NULL THEN "" ELSE createdby END AS createdby, CASE WHEN  createdOn ="0000-00-00 00:00:00" THEN NOW() WHEN  createdOn IS NULL THEN NOW() ELSE createdOn END AS createdOn, CASE WHEN  updatedby IS NULL THEN "" ELSE updatedby END AS updatedby,CASE WHEN updatedon ="0000-00-00 00:00:00" THEN NOW() WHEN  updatedon IS NULL THEN NOW() ELSE updatedon END AS updatedon, backenduptdate,process_flag,image_process_flag,popularity_comp,lowestcomp_price,preorder_flag,product_callcnt_rolling,exclusive_flag,p_variants,catid,catname,barcode,CASE WHEN barcode_list IS NULL then "" ELSE barcode_list end as barcode_list,num_aff_link,if(aff_sales_rank="999999999999","999999999",aff_sales_rank) as aff_sales_rank,aff_link_src,source_flag,product_id AS _id FROM tbl_master where product_id="%s"' %(product_id)
	#print details
	mycursor.execute(details)	
	Records = mycursor.fetchall() 

	master_arr=[]
	for master in Records: 
		product_id             = master[0] 
		product_name           = master[1] 
		actual_product_name    = master[2] 
		product_displayname    = master[3] 
		product_searchname    = master[4] 
		product_searchname_WS    = master[5] 
		product_searchname_processed    = master[6] 
		product_searchname_ignore_processed    = master[7] 
		parent_lineage    = master[8] 
		parentid_lineage    = master[9] 
		product_brandid    = master[10] 
		product_brand    = master[11] 
		product_url    = master[12] 
		product_imageurl    = master[13] 
		product_price    = master[14] 
		product_otherimages    = master[15] 
		images_crawled_path    = master[16] 
		brandcat_popularity    = master[17] 
		product_popularity    = master[18] 
		product_callcount    = master[19] 
		product_callcount_rolling    = master[20] 
		product_weight    = master[21] 
		national_catid    = master[22] 
		active_flag    = master[23] 
		autosuggest_flag    = master[24] 
		createdby    = master[25] 
		createdon    = master[26] 
		updatedby    = master[27] 
		updatedon    = master[28] 
		backenduptdate    = master[29] 
		process_flag    = master[30] 
		image_process_flag    = master[31] 
		popularity_comp    = master[32] 
		lowestcomp_price = master[33] 
		preorder_flag    = master[34] 
		product_callcnt_rolling    = master[35] 
		exclusive_flag    = master[36] 
		p_variants    = master[37] 
		catid    = master[38] 
		catname    = master[39] 
		barcode    = master[40] 
		barcode_list    = master[41] 
		num_aff_link    = master[42] 
		aff_sales_rank    = master[43] 
		aff_link_src    = master[44] 
		source_flag = master[45] 
		


		# print master
		# sys.exit

		image_details='SELECT product_imagepath as img_p,height as hgt,width as wdt,image_type as img_t,active_flag as af, updatedBy AS uby,updatedOn AS uon,backenduptdate AS bcupt FROM tbl_images where product_id="%s"' %(product_id)
		mycursor.execute(image_details)
		image_records = mycursor.fetchall()

		image_arr=[]
		for image in image_records: 
		   img_p        = image[0] 
		   hgt          = image[1] 
		   wdt          = image[2] 
		   img_t        = image[3] 
		   af         = image[4]
		   uby        = image[5]
		   uon        = image[6]
		   bcupt        = image[7]
		   image_arr.append({	"img_p" :img_p,
								"hgt"   : Decimal128(hgt),
								"wdth"   : Decimal128(wdt),
								"img_t" :img_t,
								"af" :af,
								"uby" :uby,
								"uon" :uon,
								"bcupt" :bcupt
					         })
		image_new_details='SELECT product_imagepath AS img_p,product_imagepath_fs AS imgpath_fs,product_imagepath_bsp AS imgpath_bsp,product_imagepath_tmb AS imgpath_tmb,product_imagepath_tmb_rev AS imgpath_tmb_rev,product_imagepath_tmw AS imgpath_tmw,height as hgt,width as wdth,file_name as fs,active_flag AS af,uploadedBy AS uby,updatedOn AS uon,backenduptdate AS bcupt FROM test.temp_image_new where product_id="%s"' %(product_id)
		mycursor.execute(image_new_details)
		image_new = mycursor.fetchall()

		image_new_arr=[]
		for image_new_records in image_new: 
		   img_p        = image_new_records[0] 
		   imgpath_fs    = image_new_records[1]
		   mgpath_bsp    = image_new_records[2]
		   imgpath_tmb   = image_new_records[3]
		   imgpath_tmb_rev = image_new_records[4]
		   imgpath_tmw   = image_new_records[5]
		   hgt          = image_new_records[6] 
		   wdt          = image_new_records[7] 
		   fs        = image_new_records[8]
		   af        = image_new_records[9]
		   uby        = image_new_records[10]
		   uon        = image_new_records[11]
		   bcupt        = image_new_records[12]
		   image_new_arr.append({"img_p" :img_p,
		   						"imgpath_fs" :imgpath_fs,
		   						"mgpath_bsp" :mgpath_bsp,
		   						"imgpath_tmb" :imgpath_tmb,
		   						"imgpath_tmb_rev" :imgpath_tmb_rev,
		   						"imgpath_tmw" :imgpath_tmw,
		   						"hgt"   : Decimal128(hgt),
								"wdth"   : Decimal128(wdt),
								"fs" :fs,
								"af" :af,
								"uby" :uby,
								"uon" :uon,
								"bcupt" :bcupt
								
					         }) 

			          

		#print image_arr
		#sys.exit()

		spec_details='SELECT a.spec_id AS sid,a.spec_name AS sn,a.spec_display_name AS sdn,a.spec_display_value AS sdv,cast(a.spec_numeric_value as signed) AS snv,cast(a.spec_numeric_flag as signed) AS snf,a.spec_unit AS su,cast(a.spec_active_flag as signed) AS saf,cast(a.active_flag as signed) AS af, b.sort_spec_position AS ssp,b.sort_active_flag AS sort_af,a.updatedon AS uon,a.updatedby AS uby,a.backenduptdate AS bcupt  FROM (select a.spec_id,b.spec_name,b.spec_display_name,b.spec_unit,a.spec_display_value,a.spec_numeric_value,b.spec_numeric_flag,b.active_flag AS spec_active_flag,a.active_flag,a.updatedon,a.updatedby,a.backenduptdate,a.catid FROM test.tbl_spec_display a JOIN tbl_spec_master b ON a.spec_id=b.spec_id WHERE product_id="%s") a JOIN tbl_spec_mapping b ON a.spec_id=b.spec_id AND a.catid=b.catid '%(product_id)
		#print spec_details
		#sys.exit()
		mycursor.execute(spec_details)
		spec_record = mycursor.fetchall()
		# print spec_details
		# sys.exit()

		spec_arr=[]
		for spec in spec_record: 
		   
		   sid        = spec[0] 
		   sn        = spec[1]
		   sdn        = spec[2]
		   sdv        = spec[3]
		   snv        = spec[4]
		   snf        = spec[5]
		   su        = spec[6]
		   spec_af        = spec[7]
		   paf        = spec[8]
		   ssp        = spec[9]
		   sort_af        = spec[10]
		   uon        = spec[11]
		   uby        = spec[12]
		   bcupt        = spec[13] 
		   
		   spec_arr.append({
								"sid":sid,
								"sn": sn,
								"sdn" : sdn,
								"sdv" : sdv,
								"snv" : float(snv),
								"snf" : snf,
								"su" : su,
								"saf" : spec_af,
								"paf" : paf,
								"ssp" : ssp,
								"sort_af" : sort_af,
								"uon" : uon,
								"uby" : uby,
								"bcupt" : bcupt
					         }) 


		Mongo_Collection =  {   "_id" :product_id,
								"pid" :product_id,
								"pn":product_name,
								"apn":actual_product_name,
								"pdn":product_displayname,
								"psn":product_searchname,
								"psn_ws":product_searchname_WS,
								"psnp":product_searchname_processed,
								"psn_ip":product_searchname_ignore_processed,
								 "plinage":parent_lineage,
								 "pidlineage":parentid_lineage,
								 "pbid":product_brandid,
								 "pbrand":product_brand,
								 "p_url":product_url,
								 "p_imgurl":product_imageurl,
								 "pprice":Decimal128(product_price),
								 "poimages":product_otherimages,
								 "img_cra_path":images_crawled_path,
								 "b_pop":brandcat_popularity,
								 "p_pop":product_popularity,
								 "p_cc":product_callcount,
								  "pc_rolling":Decimal128(product_callcount_rolling),
								  "pweight":product_weight,
								  "ncid":national_catid,
								  "af":active_flag,
								  "asflag":autosuggest_flag,
								  "cby":createdby,
								  "con":createdon,
								  "uptdby":updatedby,
								  "uon":updatedon,
								  "buptd":backenduptdate,
								  "pf":process_flag,
								  "img_pf":image_process_flag,
								  "p_comp":int(popularity_comp),
								  "lcp":int(lowestcomp_price),
								  "po_flag":preorder_flag,
								  "p_rolling":Decimal128(product_callcnt_rolling),
								  "ef":exclusive_flag,
								  "pv":p_variants,
								  "cid": catid,
								  "cn" : catname,
								 "bcode":barcode,
								 "bcode_list":barcode_list,
								 "num_aff_link":num_aff_link,
								 "aff_sr":int(aff_sales_rank),
								 "aff_ls":aff_link_src,
								 "s_flag": source_flag,
								 "image_info":image_arr,
								 "image_new":image_new_arr,
								"spec_values":spec_arr
		                         }
		master_arr.append(Mongo_Collection)	
	             

	
		#print(master_arr)


	mycol1.insert_many(master_arr) 
  
  

                      

                        
  


    
		


