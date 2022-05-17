#!/bin/bash
#EMAIL="yogesh.verma2@justdial.com,shravan.katti@justdial.com,anurag.singh9@justdial.com"
EMAIL="shravan.katti@justdial.com"
LOCK_FILE="/tmp/brand_connect_lookup.lock"


if [ -e ${LOCK_FILE} ]; then 
	exit 1
else
	touch ${LOCK_FILE} 

fi


function ERROR() {
        if [ $? -ne 0 ]; then
           echo "Step Failed"
           echo -e "Brand connect data processing Failed $ID $SLOT $DATE \n `cat /var/log/daily_log/brand_connect_processing.log` " | mail -s "Brand connect data processing Failed on `hostname`" ${EMAIL}
           #sh /scripts/standard_sms_sending_script.sh -m "Brand connect data processing Failed for $ID $SLOT $DATE on 192.168.42.67" -f /scripts/clickTracker.txt
           exit 1
        else
           echo "Done"  
        fi
}

echo "Step 1: getting DATA"
DATA=$(ssh $SSH_ARG -q web_backup@192.168.12.25 "sudo mysql -Bse \"SELECT id,slot,DATE FROM search_input.tbl_brand_connect_process_lookup WHERE search_flag=1 and lead_flag=1 and done_flag=0 ORDER BY DATE,slot ASC limit 1;\"")
ERROR

ID=`echo $DATA | awk '{print $1}'`
SLOT=`echo $DATA | awk '{print $2}'`
DATE=`echo $DATA | awk '{print $3}'`
VARDATE=`echo $DATE | sed -e 's/-//g'`
echo $ID $SLOT $DATE

if [ -z $DATE ]; then rm -f $LOCK_FILE ; exit 1 ; fi
ERROR


echo "Taking dump of tables on 192.168.12.25"
ssh $SSH_ARG -q web_backup@192.168.12.25 "sudo mysqldump search_input tbl_brand_connect_search_summary_${VARDATE}_${SLOT} > /tmp/tbl_brand_connect_search_summary_${VARDATE}_${SLOT}.sql && gzip -f /tmp/tbl_brand_connect_search_summary_${VARDATE}_${SLOT}.sql"
ERROR
ssh $SSH_ARG -q web_backup@192.168.12.25 "sudo mysqldump search_input tbl_brand_connect_lead_summary_${VARDATE}_${SLOT} > /tmp/tbl_brand_connect_lead_summary_${VARDATE}_${SLOT}.sql && gzip -f /tmp/tbl_brand_connect_lead_summary_${VARDATE}_${SLOT}.sql"
ERROR
ssh $SSH_ARG -q web_backup@192.168.12.25 "sudo mysqldump search_input tbl_brand_connect_platform_cnts_${VARDATE}_${SLOT} > /tmp/tbl_brand_connect_platform_cnts_${VARDATE}_${SLOT}.sql && gzip -f /tmp/tbl_brand_connect_platform_cnts_${VARDATE}_${SLOT}.sql"
ERROR


echo "Copying tables from 192.168.12.25 to 192.168.42.67"
scp ${SSH_ARGS} web_backup@192.168.12.25:/tmp/tbl_brand_connect_search_summary_${VARDATE}_${SLOT}.sql.gz /tmp/
ERROR
scp ${SSH_ARGS} web_backup@192.168.12.25:/tmp/tbl_brand_connect_lead_summary_${VARDATE}_${SLOT}.sql.gz /tmp/
ERROR
scp ${SSH_ARGS} web_backup@192.168.12.25:/tmp/tbl_brand_connect_platform_cnts_${VARDATE}_${SLOT}.sql.gz /tmp/
ERROR


echo "gunzipping and restoration on 192.168.42.67"
gunzip -f /tmp/tbl_brand_connect_search_summary_${VARDATE}_${SLOT}.sql.gz && sudo mysql search_input < /tmp/tbl_brand_connect_search_summary_${VARDATE}_${SLOT}.sql &&
ERROR
gunzip -f /tmp/tbl_brand_connect_lead_summary_${VARDATE}_${SLOT}.sql.gz && sudo mysql search_input < /tmp/tbl_brand_connect_lead_summary_${VARDATE}_${SLOT}.sql
ERROR
gunzip -f /tmp/tbl_brand_connect_platform_cnts_${VARDATE}_${SLOT}.sql.gz && sudo mysql search_input < /tmp/tbl_brand_connect_platform_cnts_${VARDATE}_${SLOT}.sql
ERROR


echo "Removing files"
ssh -q $SSH_ARGS web_backup@192.168.12.25 "sudo rm -f /tmp/tbl_brand_connect_search_summary_${VARDATE}_${SLOT}.sql.gz /tmp/tbl_brand_connect_lead_summary_${VARDATE}_${SLOT}.sql.gz /tmp/tbl_brand_connect_platform_cnts_${VARDATE}_${SLOT}.sql.gz"
ERROR
sudo rm -f /tmp/tbl_brand_connect_search_summary_${VARDATE}_${SLOT}.sql /tmp/tbl_brand_connect_lead_summary_${VARDATE}_${SLOT}.sql /tmp/tbl_brand_connect_platform_cnts_${VARDATE}_${SLOT}.sql
ERROR


echo "Calling SP sp_brand_connect_summary"
mysql -Bse "call search_input.sp_brand_connect_summary('$DATE','$SLOT');"
ERROR


echo "Calling SP sp_brand_connect_summary on 192.168.12.25"
ssh $SSH_ARG -q web_backup@192.168.12.25 "sudo mysql -Bse \"call search_input.sp_brand_connect_tbl_deletion('$DATE','$SLOT');\""
ERROR


echo "Updating Flag to 1"
ssh $SSH_ARG -q web_backup@192.168.12.25 "sudo mysql -Bse \"update search_input.tbl_brand_connect_process_lookup set done_flag=1 where id=${ID};\""

echo "Removing the lock file"
rm -f ${LOCK_FILE}

echo "################# $ID $SLOT $DATE Brand connect process Success --- Proces END at `date` ################" 


