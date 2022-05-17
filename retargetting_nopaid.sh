#!/bin/bash

SSH_ARG='-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q'
DATE=`date +%Y%m%d`
search_date=`date +%Y-%m-%d`
search_time=`date +"%H:%M:%S"`
slot=`date +%H%M`
EMAILIDS="nancy.brijwani@justdial.com,vipul.singh1@justdial.com"

cleanup() {
echo "removing dump files"
echo "Removing txt on 12.218 tbl_retargetting_c2l_non_paid_"
ssh $SSH_ARG web_backup@192.168.12.218 "sudo rm -fv /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt"
ERROR
echo "Removing txt on 11.151 tbl_retargetting_c2l_non_paid_"
ssh $SSH_ARG web_backup@192.168.11.151 "sudo rm -fv /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt"
ERROR



}

ERROR () {
        if [ $? -ne 0 ]; then
                echo "Step Failed"
                echo "sp_retargetting_non_paid_generation failed for $search_time on $search_date slot:$slot" | mail -s "sp_retargetting_non_paid_generation failed for $search_time@$search_date" ${EMAILIDS}
              cleanup
            #  sh /scripts/standard_sms_sending_script.sh -m "sp_retargetting_non_paid_generation failed for $search_time on $search_date slot:$slot on 12.25" -f /scripts/clickTracker.txt
                exit 1
        fi
}

SUCCESS() {

if [ $? -eq 0 ]; then
                echo "Process sp_retargetting_non_paid_generation successfully completed for $search_time on $search_date slot:$slot"
                echo "sp_retargetting_non_paid_generation completed for $search_time on $search_date slot:$slot" | mail -s "sp_retargetting_non_paid_generation completed $search_time@$search_date" ${EMAILIDS}
		cleanup
                exit 0
        fi
}
#DATE="20170627"
#search_date="2017-06-27"
#search_time="12:45:00"
#slot="1245"
#############################################################################
LOCK_FILE="/tmp/retargetting_non_paid_generation.lock"
if [ -e ${LOCK_FILE} ];then echo " Process sp_retargetting_non_paid_generation already running..." && exit 1
else
echo "Creating lock file ${LOCK_FILE}"
lockfile -r0 ${LOCK_FILE}
fi
trap "rm -f $LOCK_FILE" SIGINT SIGTERM EXIT
#############################################################################



TIME=$(date +"%H:%M")
if [ "$TIME" == "00:00" ] || [ "$TIME" == "00:01" ] || [ "$TIME" == "00:02" ] || [ "$TIME" == "00:03" ] || [ "$TIME" == "00:04" ] || [ "$TIME" == "00:05" ]; then
        DATE=`date +%Y%m%d -d "yesterday"`
        search_date=`date +%Y-%m-%d -d "yesterday"`
        search_time='23:59:59'
        slot='9999'
        
fi

if [[ $slot -eq "9999" ]] ;then 
 
sleep 300

fi
echo "calling sp sp_retargetting_non_paid_generation on 12.218"
ssh $SSH_ARG web_backup@192.168.12.218 "sudo mysql -e 'CALL justdial.sp_retargetting_non_paid_generation(\"$search_date\",\"$search_time\",\"$slot\") ;' "
ERROR


before_cnt=`ssh $SSH_ARG web_backup@192.168.12.218 "cat /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt | wc -l"`
ERROR

echo "Inserting tbl_slot_processing_lookup_c2l_nonpaid into search_input "

sudo mysql -e "INSERT ignore  INTO  search_input.tbl_slot_processing_lookup_c2l_nonpaid (entry_date,slot,before_cnt,start_time,done_flag) values (\"$search_date\", \"$slot\", \"$before_cnt\", now(),1);"
ERROR


echo "copying the infile to 12.25"

sudo scp $SSH_ARG -q web_backup@192.168.12.218:/tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt /tmp/ 
ERROR

#ssh $SSH_ARG  web_backup@192.168.12.218 "sudo scp $SSH_ARG /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt web_backup@192.168.12.26:/tmp" 
#ERROR

sudo chown mysql.mysql /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt
ERROR

mysql -e "LOAD DATA INFILE '/tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt'  IGNORE INTO TABLE search_input.tbl_retargetting_c2l_nonpaid FIELDS TERMINATED BY '|'  ENCLOSED BY '\"' LINES TERMINATED BY '\n'  (lead_id,id,input,user_ip,city,area,search_term,catid,national_catid,catname,docid,compname,jduid,mobile,email,pincode,doc_display_flag,src,search_date,search_time,name,slot_time,page_no,pos,group_id,remarks,done_flag,flg,ctf,paid_flag,magic_name,ndnc_no,vendor_salutation,email_txt,lead_flag,convo_txt,landline,clinum,platform,grab_flag,device_src,search_type);"
ERROR


a_cnt=`sudo mysql -Bse 'select count(1) as cnt  from search_input.tbl_retargetting_c2l_nonpaid;'`
echo $a_cnt
ERROR


echo "updating the tbl after count"

mysql -e "UPDATE  search_input.tbl_slot_processing_lookup_c2l_nonpaid SET done_flag=2 , after_cnt=${a_cnt}  where entry_date=${search_date} and slot=${slot};"
ERROR


echo "calling sp on 12.25 sp_lead_data_insertion"

mysql -e "CALL  search_input.sp_lead_nonpaid_data_insertion(\"$DATE\",\"$slot\");"
ERROR


#sudo scp $SSH_ARG -q /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt web_backup@192.168.11.151:/var/lib/mysql-files/
#ERROR

sudo scp $SSH_ARG -q /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt web_backup@192.168.11.151:/tmp/

ERROR


ssh $SSH_ARG web_backup@192.168.11.151 "sudo mv /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt /var/lib/mysql-files/ && sudo chown mysql.mysql /var/lib/mysql-files/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt"

ERROR

#ssh $SSH_ARG web_backup@192.168.11.151 "sudo rm -fv /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt"
#ERROR


ssh $SSH_ARG web_backup@192.168.11.151 "sudo mysql -e \"LOAD DATA LOCAL INFILE '/var/lib/mysql-files/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt' IGNORE INTO TABLE search_input.tbl_retargetting_c2l_nonpaid FIELDS TERMINATED BY '|'  ENCLOSED BY '\\\"' LINES TERMINATED BY '\n' (lead_id,id,input,user_ip,city,area,search_term,catid,national_catid,catname,docid,compname,jduid,mobile,email,pincode,doc_display_flag,src,search_date,search_time,name,slot_time,page_no,pos,group_id,remarks,done_flag,flg,ctf,paid_flag,magic_name,ndnc_no,vendor_salutation,email_txt,lead_flag,convo_txt,landline,clinum,platform,grab_flag,device_src,search_type);\""


#echo $QUERY > /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt
#sed -i 's/^/"/;s/$/"/;s/^/mysql -uroot -e /' /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt


ssh $SSH_ARG web_backup@192.168.11.151 "mysql -uroot -e \"CALL  search_input.sp_lead_nonpaid_insertion(${DATE},${slot});\""

#echo $SP >> /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt
#scp $SSH_ARG /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt web_backup@192.168.11.151:/tmp/
#ERROR

ssh $SSH_ARG web_backup@192.168.11.151 "sudo rm -fv  /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt"
ERROR

echo "Removing txt on 12.25 tbl_retargetting_c2l_non_paid_ "
sudo rm -fv /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt 
ERROR
#
ssh $SSH_ARG web_backup@192.168.12.218 "sudo rm -fv /tmp/tbl_retargetting_c2l_non_paid_${DATE}${slot}.txt"
ERROR


SUCCESS

