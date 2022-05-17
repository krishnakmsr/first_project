

#########################################

#!/bin/bash
SSH_ARGS='-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q'
LOCK_FILE="/tmp/clients_processing_normz.lock"
EMAIL="yogesh.verma2@justdial.com,nancy.brijwani@justdial.com,vipul.singh1@justdial.com"

if [ -e ${LOCK_FILE} ]; then
        exit 1
else
        touch ${LOCK_FILE}

fi

function ERROR() {
        if [ $? -ne 0 ]; then
                echo "Step Failed"
                echo -e " clients_processing_normz Failed on 12.25 $ID $SLOT $DATE" | mail -s " clients_processing_normz Process Failed on 12.26 `hostname`" ${EMAIL}
                /bin/bash /scripts/standard_sms_sending_script.sh -m "$0 failed for $DATA on 192.168.12.25" -f /scripts/tbl_websearches_clients.txt
#                rm -f ${LOCK_FILE}
                exit 1
        else
                echo "Done"
        fi
}


echo "Step 1: getting DATA"
DATA=`mysql -Bse "SELECT id,slot,DATE FROM search_input.clients_processing_normz_lookup WHERE done_flag=0 ORDER BY DATE,slot ASC LIMIT 1"`
ERROR

VARID=`echo $DATA | awk '{print $1}'`
VARSLOT=`echo $DATA | awk '{print $2}'`
DATE=`echo $DATA | awk '{print $3}'`
VARDATE=`echo $DATE | sed -e 's/-//g'`
echo $VARID $VARSLOT $DATE $VARDATE



if [ -z $DATE ]; then rm -f $LOCK_FILE ; exit 1 ; fi
ERROR





#########################


echo "step 0.1: calling sp_client_summary_normz_table_creation_part1"
mysql -uroot -Bse "CALL search_input.sp_client_summary_normz_table_creation_part1('$DATE','$VARSLOT');"
ERROR


echo "step 0.2: calling python script"
python2.6 /scripts/dataNormalization.py
ERROR

echo "step 0.3"
mysql -Bse "LOAD DATA INFILE '/tmp/post_normz_websearches.csv' IGNORE INTO TABLE search_input.temp_post_normz_websearches  FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' (auto_id,lead_id,docid);"
ERROR

echo "step 0.4: calling sp_client_summary_normz_table_creation_part2"
mysql -uroot -Bse "CALL search_input.sp_client_summary_normz_table_creation_part2('$DATE','$VARSLOT');"
ERROR


echo "step 0.5: removing pre_normz_websearches post_normz_websearches file"
rm -fv /tmp/pre_normz_websearches.csv /tmp/post_normz_websearches.csv
ERROR

#########################

echo "Removing the lock file"
rm -f ${LOCK_FILE}

echo "################# $ID $SLOT $DATE Success at search_input.clients_processing_normz Proces END at `date` ################"
echo "clients_processing_normz Proces $ID $SLOT $DATE Success on `hostname` at `date`" | mail -s "clients_processing_normz Process $ID $SLOT $DATE Succees on `hostname` at `date`" ${EMAIL}

#########################################
