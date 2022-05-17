#!/bin/bash
SSH_ARGS='-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q'
LOCK_FILE="/tmp/clients_processing_counts.lock"
EMAIL="rishu.gupta1@justdial.com,yogesh.verma2@justdial.com,nancy.brijwani@justdial.com,vipul.singh1@justdial.com"

if [ -e ${LOCK_FILE} ]; then
        exit 1
else
        touch ${LOCK_FILE}

fi

function ERROR() {
        if [ $? -ne 0 ]; then
                echo "Step Failed"
                echo -e " tbl_websearches_clients_processing_cnts_compressed Failed $ID $SLOT $DATE" | mail -s " tbl_websearches_clients_processing_cnts_compressed Process Failed on `hostname`" ${EMAIL}
                /bin/bash /scripts/standard_sms_sending_script.sh -m "$0 failed for $ID $SLOT $DATE $DATA on 192.168.12.25" -f /scripts/clickTracker.txt
#                rm -f ${LOCK_FILE}
                exit 1
        else
                echo "Done"  
        fi
}


echo "Step 1: getting DATA"
DATA=`mysql -Bse "SELECT id,slot,DATE FROM search_input.clients_processing_cnts_table_transfer_lookup WHERE done_flag=0 ORDER BY DATE,slot ASC LIMIT 1"`
ERROR

VARID=`echo $DATA | awk '{print $1}'`
VARSLOT=`echo $DATA | awk '{print $2}'`
DATE=`echo $DATA | awk '{print $3}'`
VARDATE=`echo $DATE | sed -e 's/-//g'`
echo $VARID $VARSLOT $DATE $VARDATE

#echo "step 1.1: call sp for client summary modified data sp_client_summary_modified"
#mysql -uroot -Bse "CALL search_input.sp_client_summary_modified('$VARSLOT','$DATE');"
#ERROR
#
if [ -z $DATE ]; then rm -f $LOCK_FILE ; exit 1 ; fi
ERROR

echo "step 1.1: call sp for client summary modified data sp_client_summary_modified"
mysql -uroot -Bse "CALL search_input.sp_client_summary_modified('$VARSLOT','$DATE');"
ERROR


echo "step 1.2: call sp for search_input.sp_websearches_catid_pincode_count_compressed_1"
mysql -uroot -Bse "CALL search_input.sp_websearches_catid_pincode_count_compressed_1('$DATE','$VARSLOT');"
ERROR
#
#echo "step 1.3: call sp for search_input.sp_websearches_count_compressed_1"
#mysql -uroot -Bse "CALL search_input.sp_websearches_count_compressed_1('$VARSLOT','$DATE');"
#ERROR
#

echo "Step 2 : Run query on 12.25"
mysql -Bse "UPDATE search_input.clients_processing_cnts_table_transfer_lookup SET start_time = NOW() WHERE id = ${VARID};"
ERROR

echo "Step : 3 :- transfer TABLE from 12.25 to 11.149"
mysqldump search_input tbl_websearches_clients_processing_cnts_compressed_${VARDATE}_${VARSLOT} > /tmp/tbl_websearches_clients_processing_cnts_compressed_${VARDATE}_${VARSLOT}.sql
ERROR

gzip -f /tmp/tbl_websearches_clients_processing_cnts_compressed_${VARDATE}_${VARSLOT}.sql
ERROR

scp /tmp/tbl_websearches_clients_processing_cnts_compressed_${VARDATE}_${VARSLOT}.sql.gz web_backup@192.168.11.149:/tmp
ERROR

ssh -q $SSH_ARGS web_backup@192.168.11.149 "gunzip -f /tmp/tbl_websearches_clients_processing_cnts_compressed_${VARDATE}_${VARSLOT}.sql.gz"
ERROR

ssh -q $SSH_ARGS web_backup@192.168.11.149 "sudo mysql test < /tmp/tbl_websearches_clients_processing_cnts_compressed_${VARDATE}_${VARSLOT}.sql"
ERROR

VAR1=`mysql -Bse "select count(1) from search_input.tbl_websearches_clients_processing_cnts_compressed_${VARDATE}_${VARSLOT};"`
ERROR
VAR2=`ssh -q $SSH_ARGS web_backup@192.168.11.149 "sudo mysql -Bse 'select count(1) from test.tbl_websearches_clients_processing_cnts_compressed_${VARDATE}_${VARSLOT};'"`
ERROR

echo $VAR1 $VAR2 
if [ $VAR1 -eq $VAR2 ]; then
	echo "Update done flag 1 in table search_input.clients_processing_cnts_table_transfer_lookup"
	echo "Step : 4 :- DATA counts FROM BOTH SERVER AND UPDATE IN TABLE BY running QUERY"
	mysql -Bse "UPDATE search_input.clients_processing_cnts_table_transfer_lookup SET table_cnt_1 = ${VAR1}, table_cnt_2 = ${VAR2}, end_time = NOW(), done_flag = 1 WHERE id = ${VARID};"
	ERROR
	echo "Checking done flag in table search_input.tbl_views_push_notification_table_transfer_lookup"
	DONE_FLAG=`mysql -Bse "select done_flag from search_input.tbl_views_push_notification_table_transfer_lookup where date = '${DATE}' and slot = ${VARSLOT};"`
	if [ $DONE_FLAG -eq 1 ]; then
		mysql -Bse "DROP TABLE IF EXISTS search_input.tbl_websearches_clients_processing_cnts_compressed_${VARDATE}_${VARSLOT};"     
		ERROR
	fi
else
	
	echo "Update done flag 2 in table search_input.clients_processing_cnts_table_transfer_lookup"
	echo "Step : 4 :- DATA counts FROM BOTH SERVER AND UPDATE IN TABLE BY running QUERY"
	mysql -Bse "UPDATE search_input.clients_processing_cnts_table_transfer_lookup SET table_cnt_1 = ${VAR1}, table_cnt_2 = ${VAR2}, end_time = NOW(), done_flag = 2 WHERE id = ${VARID};"
        ERROR
	echo "count not matched"
fi
#
#if [ $VAR1 -eq $VAR2 ]; then
#   echo "Table tbl_websearches_clients_processing_cnts_compressed_${VARDATE}_${VARSLOT} dropping on 12.25"
#   mysql -Bse "DROP TABLE IF EXISTS search_input.tbl_websearches_clients_processing_cnts_compressed_${VARDATE}_${VARSLOT};" 	
#   ERROR
#   else 
#   echo "count not matched"
#   ERROR
#fi 
echo "Removing sql files on 12.25"
sudo rm -fv /tmp/tbl_websearches_clients_processing_cnts_compressed_${VARDATE}_${VARSLOT}.sql.gz
ERROR

echo "Removing sql files on 11.149"
ssh -q $SSH_ARGS web_backup@192.168.11.149 "sudo rm -fv /tmp/tbl_websearches_clients_processing_cnts_compressed_${VARDATE}_${VARSLOT}.sql"
ERROR

echo "Inserting date and slot into search_input.tbl_push_notification_lookup on 192.168.11.149"
ssh -q $SSH_ARGS web_backup@192.168.11.149 "sudo mysql -Bse 'INSERT IGNORE INTO search_input.tbl_push_notification_lookup (slot,date) VALUES(\"$VARSLOT\",\"$DATE\") ;'" 
ERROR

echo "Removing the lock file"
rm -f ${LOCK_FILE}

echo "################# $ID $SLOT $DATE Success at search_input.tbl_websearches_clients_processing_cnts_compressed Proces END at `date` ################" 
echo "tbl_websearches_clients_processing_cnts_compressed Proces $ID $SLOT $DATE Success on `hostname` at `date`" | mail -s "tbl_websearches_clients_processing_cnts_compressed Process $ID $SLOT $DATE Succees on `hostname` at `date`" ${EMAIL}
    



