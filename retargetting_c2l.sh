#!/bin/bash

SSH_ARG='-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q'
DATE=`date +%Y%m%d`
search_date=`date +%Y-%m-%d`
search_time=`date +"%H:%M:%S"`
slot=`date +%H%M`
EMAILIDS="yogesh.verma2@justdial.com,nancy.brijwani@justdial.com,shravan.katti@justdial.com,anurag.singh9@justdial.com"

cleanup() {
echo "removing dump files"
echo "Removing txt on 12.218 and 11.151 tbl_retargetting_c2l"
ssh $SSH_ARG web_backup@192.168.12.218 "sudo rm -fv /tmp/tbl_retargetting_c2l_${DATE}${slot}.txt"
ERROR
ssh $SSH_ARG web_backup@192.168.11.151 "sudo rm -fv /var/lib/mysql-files/tbl_retargetting_c2l_${DATE}${slot}.txt"
ERROR

echo "Removing txt on 12.25 tbl_retargetting_c2l and tbl_coc_nonpaid_data"
rm -fv /tmp/tbl_retargetting_c2l_${DATE}${slot}.txt /tmp/tbl_coc_nonpaid_data_${DATE}${slot}.txt
ERROR

#echo "Removing txt on 12.26 tbl_retargetting_c2l and tbl_coc_nonpaid_data"
#ssh $SSH_ARG web_backup@192.168.12.26 "sudo rm -fv /tmp/tbl_retargetting_c2l_${DATE}${slot}.txt /tmp/tbl_coc_nonpaid_data_${DATE}${slot}.txt"
#ERROR26

echo "Removing txt on 1.8 tbl_coc_nonpaid_data"
ssh $SSH_ARG web_backup@192.168.1.8 "sudo rm -fv /tmp/tbl_coc_nonpaid_data_${DATE}${slot}.txt"
ERROR


}

ERROR () {
        if [ $? -ne 0 ]; then
                echo "Step Failed"
                echo "tbl_retargetting failed for $search_time on $search_date slot:$slot" | mail -s "tbl_retargetting failed for $search_time@$search_date" ${EMAILIDS}
              cleanup
              sh /scripts/standard_sms_sending_script.sh -m "tbl_retargetting failed for $search_time on $search_date slot:$slot on 12.25" -f /scripts/clickTracker.txt
                exit 1
        fi
}
ERROR26 () {
        if [ $? -ne 0 ]; then
                echo "Step Failed on 12.26"
                echo -e "tbl_retargetting failed for $search_time on $search_date slot:$slot for 12.26\nMail source:12.25\nscript:$0" | mail -s "tbl_retargetting failed 12.26 for $search_time@$search_date" ${EMAILIDS}
                sh /scripts/standard_sms_sending_script.sh -m "tbl_retargetting failed for $search_time on $search_date slot:$slot for 12.26" -f /scripts/clickTracker.txt
        fi
}

SUCCESS() {

if [ $? -eq 0 ]; then
                echo "Process tbl_retargetting successfully completed for $search_time on $search_date slot:$slot"
                echo "tbl_retargetting completed for $search_time on $search_date slot:$slot" | mail -s "tbl_retargetting completed $search_time@$search_date" ${EMAILIDS}
		cleanup
                exit 0
        fi
}
#DATE="20170627"
#search_date="2017-06-27"
#search_time="12:45:00"
#slot="1245"
#############################################################################
LOCK_FILE="/tmp/retargetting.lock"
if [ -e ${LOCK_FILE} ];then echo " Process tbl_retargetting already running..." && exit 1
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

echo "calling sp sp_retargetting_generation on 12.218"
ssh $SSH_ARG web_backup@192.168.12.218 "sudo mysql -e 'CALL justdial.sp_retargetting_generation(\"$search_date\",\"$search_time\",\"$slot\") ;' "
ERROR

echo "calling sp sp_coc_nonpaid_data_generation on 1.8"
ssh $SSH_ARG web_backup@192.168.1.8 "sudo mysql -e 'CALL websmsemail.sp_coc_nonpaid_data_generation(\"$search_date\",\"$search_time\",\"$slot\") ;' "
ERROR
before_cnt=`ssh $SSH_ARG web_backup@192.168.12.218 "cat /tmp/tbl_retargetting_c2l_${DATE}${slot}.txt | wc -l"`
ERROR
before_cnt_1=`ssh $SSH_ARG web_backup@192.168.1.8 "cat /tmp/tbl_coc_nonpaid_data_${DATE}${slot}.txt | wc -l"`
ERROR
echo "Inserting tbl_slot_processing_lookup_c2l into search_input "

sudo mysql -e "INSERT ignore  INTO  search_input.tbl_slot_processing_lookup_c2l (entry_date,slot,before_cnt,start_time,done_flag) values (\"$search_date\", \"$slot\", \"$before_cnt\", now(),1);"
ERROR

sudo mysql -e "INSERT ignore  INTO  search_input.tbl_slot_processing_lookup_coc_nonpaid (date,slot,before_cnt,start_time,done_flag) values (\"$search_date\", \"$slot\", \"$before_cnt_1\", now(),1);"
ERROR

#ssh $SSH_ARG web_backup@192.168.12.26 "sudo mysql -e 'INSERT ignore  INTO  search_input.tbl_slot_processing_lookup_c2l (entry_date,slot,before_cnt,start_time,done_flag) values (\"$search_date\", \"$slot\", \"$before_cnt\", now(),1);'"
#ERROR26

#ssh $SSH_ARG web_backup@192.168.12.26 "sudo mysql -e 'INSERT ignore  INTO  search_input.tbl_slot_processing_lookup_coc_nonpaid (date,slot,before_cnt,start_time,done_flag) values (\"$search_date\", \"$slot\", \"$before_cnt_1\", now(),1);'"
#ERROR26

echo "copying the infile to 12.25"

sudo scp $SSH_ARG -q web_backup@192.168.12.218:/tmp/tbl_retargetting_c2l_${DATE}${slot}.txt /tmp/ 
ERROR

#ssh $SSH_ARG  web_backup@192.168.12.218 "sudo scp $SSH_ARG /tmp/tbl_retargetting_c2l_${DATE}${slot}.txt web_backup@192.168.12.26:/tmp" 
#ERROR

sudo scp $SSH_ARG -q web_backup@192.168.1.8:/tmp/tbl_coc_nonpaid_data_${DATE}${slot}.txt /tmp/
ERROR

#ssh $SSH_ARG web_backup@192.168.1.8 "sudo scp $SSH_ARG /tmp/tbl_coc_nonpaid_data_${DATE}${slot}.txt web_backup@192.168.12.26:/tmp" 
#ERROR26

sudo chown mysql.mysql /tmp/tbl_retargetting_c2l_${DATE}${slot}.txt
ERROR
sudo chown mysql.mysql /tmp/tbl_coc_nonpaid_data_${DATE}${slot}.txt
ERROR

mysql -e "LOAD DATA INFILE '/tmp/tbl_retargetting_c2l_${DATE}${slot}.txt'  IGNORE INTO TABLE search_input.tbl_retargetting_c2l FIELDS TERMINATED BY '|'  ENCLOSED BY '\"' LINES TERMINATED BY '\n'  (lead_id,id,input,user_ip,city,area,search_term,catid,national_catid,catname,docid,compname,jduid,mobile,email,pincode,doc_display_flag,src,search_date,search_time,name,slot_time,page_no,pos,group_id,remarks,done_flag,flg,ctf,paid_flag,magic_name,ndnc_no,vendor_salutation,email_txt,lead_flag,convo_txt,landline,clinum,platform,grab_flag,device_src,search_type);"
ERROR
mysql -e "LOAD DATA INFILE '/tmp/tbl_coc_nonpaid_data_${DATE}${slot}.txt'  IGNORE INTO TABLE search_input.tbl_coc_nonpaid_data FIELDS TERMINATED BY '|'  ENCLOSED BY '\"' LINES TERMINATED BY '\n'  (Id,log_id,Contract,docid,Class,ClassName,Date,Time,FDate,FTime,Prefix,Caller,Phone,Email,CallerIdT,CallerIdM,Company,CompanyFax,CompanyMobile,CompanyEmail,InstantEmail,Sent,SMSSent,BulkMailSent,CallerRequestMail,MultiCity,SMSErrorCode,EmailErorCode,pincode,MachineNumber,CallerFeedBack,UID,Server,EmailSentTime,SMSSentTime,Line1,Line2,MailUID,LDFlag,CallerAreaName,AreaName,LDTotal,LDBal,Bcaller,OrderId,RmFlag,CityName,EmailSubject,EmailText,SmsText,EmailOperator,SMSTryDone,EmailTryDone,EmailCreatDate,SMSCreateDate,EmailCreateTime,SMSCreateTime,EmailSentDate,SMSSentDate,ContractType,number_masking,source,MsgId,lead_deduct,vn_processed);"
ERROR

#ssh $SSH_ARG web_backup@192.168.12.26 "sudo chown mysql.mysql /tmp/tbl_retargetting_c2l_${DATE}${slot}.txt  /tmp/tbl_coc_nonpaid_data_${DATE}${slot}.txt"
#ERROR26
a="'"
b='"'
c="\\"

query="sudo mysql -e  $b$c$b LOAD DATA INFILE $a/tmp/tbl_coc_nonpaid_data_${DATE}${slot}.txt$a  IGNORE INTO TABLE search_input.tbl_coc_nonpaid_data FIELDS TERMINATED BY $a|$a  ENCLOSED BY $a$c$c$c$b$a LINES TERMINATED BY '\n'  (Id,log_id,Contract,docid,Class,ClassName,Date,Time,FDate,FTime,Prefix,Caller,Phone,Email,CallerIdT,CallerIdM,Company,CompanyFax,CompanyMobile,CompanyEmail,InstantEmail,Sent,SMSSent,BulkMailSent,CallerRequestMail,MultiCity,SMSErrorCode,EmailErorCode,pincode,MachineNumber,CallerFeedBack,UID,Server,EmailSentTime,SMSSentTime,Line1,Line2,MailUID,LDFlag,CallerAreaName,AreaName,LDTotal,LDBal,Bcaller,OrderId,RmFlag,CityName,EmailSubject,EmailText,SmsText,EmailOperator,SMSTryDone,EmailTryDone,EmailCreatDate,SMSCreateDate,EmailCreateTime,SMSCreateTime,EmailSentDate,SMSSentDate,ContractType,number_masking,source,MsgId,lead_deduct,vn_processed);$b$c$b"

#echo "Executing query on 12.26"
#ssh $SSH_ARG web_backup@192.168.12.26 "echo $query|sh"
#ERROR26

QUERY="sudo mysql -e  $b$c$b LOAD DATA INFILE $a/tmp/tbl_retargetting_c2l_${DATE}${slot}.txt$a  IGNORE INTO TABLE search_input.tbl_retargetting_c2l FIELDS TERMINATED BY $a|$a  ENCLOSED BY $a$c$c$c$b$a  LINES TERMINATED BY '\n'  (lead_id,id,input,user_ip,city,area,search_term,catid,national_catid,catname,docid,compname,jduid,mobile,email,pincode,doc_display_flag,src,search_date,search_time,name,slot_time,page_no,pos,group_id,remarks,done_flag,flg,ctf,paid_flag,magic_name,ndnc_no,vendor_salutation,email_txt,lead_flag,convo_txt,landline,clinum,platform);$b$c$b"

#echo "Executing QUERY on 12.26"
#ssh $SSH_ARG web_backup@192.168.12.26 "echo $QUERY|sh"
#ERROR26

a_cnt=`sudo mysql -Bse 'select count(1) as cnt  from search_input.tbl_retargetting_c2l;'`
echo $a_cnt
ERROR
a_cnt_1=`sudo mysql -Bse 'select count(1) as cnt  from search_input.tbl_coc_nonpaid_data;'`
echo $a_cnt_1
ERROR
echo "updating the tbl after count"

mysql -e "UPDATE  search_input.tbl_slot_processing_lookup_c2l SET done_flag=2 , after_cnt=${a_cnt}  where entry_date=${search_date} and slot=${slot};"
ERROR
mysql -e "UPDATE  search_input.tbl_slot_processing_lookup_coc_nonpaid SET done_flag=2 , after_cnt=${a_cnt_1}  where date=${search_date} and slot=${slot};"
ERROR

#ssh $SSH_ARG web_backup@192.168.12.26 "sudo mysql -e 'UPDATE  search_input.tbl_slot_processing_lookup_c2l SET done_flag=2 , after_cnt=${a_cnt}  where entry_date=${search_date} and slot=${slot};'"
#ERROR

#ssh $SSH_ARG web_backup@192.168.12.26 "sudo mysql -e 'UPDATE  search_input.tbl_slot_processing_lookup_coc_nonpaid SET done_flag=2 , after_cnt=${a_cnt_1}  where date=${search_date} and slot=${slot};'"
#ERROR

echo "calling sp on 12.25 sp_lead_data_insertion"

mysql -e "CALL  search_input.sp_lead_data_insertion(\"$DATE\",\"$slot\");"
ERROR
echo "calling sp on 12.25 sp_coc_nonpaid_data_insertion"
mysql -e "CALL  search_input.sp_coc_nonpaid_data_insertion(\"$DATE\",\"$slot\");"
ERROR

#echo "calling sp on 12.26 sp_lead_data_insertion"
#ssh $SSH_ARG web_backup@192.168.12.26 "sudo mysql -e 'CALL  search_input.sp_lead_data_insertion(\"$DATE\",\"$slot\");'"
#ERROR26
#echo "calling sp on 12.26 sp_coc_nonpaid_data_insertion"
#ssh $SSH_ARG web_backup@192.168.12.26 "sudo mysql -e 'CALL  search_input.sp_coc_nonpaid_data_insertion(\"$DATE\",\"$slot\");'"
#ERROR26


sudo scp $SSH_ARG -q /tmp/tbl_retargetting_c2l_${DATE}${slot}.txt web_backup@192.168.11.151:/tmp/
ERROR
ssh $SSH_ARG web_backup@192.168.11.151 "sudo mv /tmp/tbl_retargetting_c2l_${DATE}${slot}.txt /var/lib/mysql-files/ && sudo chown mysql.mysql /var/lib/mysql-files/tbl_retargetting_c2l_${DATE}${slot}.txt" 
ERROR
#mysql -e "LOAD DATA INFILE '/tmp/tbl_retargetting_c2l_${DATE}${slot}.txt'  IGNORE INTO TABLE search_input.tbl_retargetting_c2l FIELDS TERMINATED BY '|'  ENCLOSED BY '\"' LINES TERMINATED BY '\n'  (lead_id,id,input,user_ip,city,area,search_term,catid,national_catid,catname,docid,compname,jduid,mobile,email,pincode,doc_display_flag,src,search_date,search_time,name,slot_time,page_no,pos,group_id,remarks,done_flag,flg,ctf,paid_flag,magic_name,ndnc_no,vendor_salutation,email_txt,lead_flag,convo_txt,landline,clinum);"

ssh $SSH_ARG web_backup@192.168.11.151 "sudo rm -fv /tmp/query.txt"
ERROR
QUERY="LOAD DATA LOCAL INFILE '/var/lib/mysql-files/tbl_retargetting_c2l_${DATE}${slot}.txt'  IGNORE INTO TABLE search_input.tbl_retargetting_c2l FIELDS TERMINATED BY '|'  ENCLOSED BY '\\\"' LINES TERMINATED BY '\n'  (lead_id,id,input,user_ip,city,area,search_term,catid,national_catid,catname,docid,compname,jduid,mobile,email,pincode,doc_display_flag,src,search_date,search_time,name,slot_time,page_no,pos,group_id,remarks,done_flag,flg,ctf,paid_flag,magic_name,ndnc_no,vendor_salutation,email_txt,lead_flag,convo_txt,landline,clinum,platform);"

echo $QUERY > /tmp/query.txt
sed -i 's/^/"/;s/$/"/;s/^/mysql -uroot -e /' /tmp/query.txt
SP="mysql -uroot -e \"CALL  search_input.sp_lead_data_insertion(\"$DATE\",\"$slot\");\""
echo $SP >> /tmp/query.txt
scp $SSH_ARG /tmp/query.txt web_backup@192.168.11.151:/tmp/
ERROR
ssh $SSH_ARG web_backup@192.168.11.151 "sudo sh /tmp/query.txt"
ERROR

#echo "Removing txt on 12.25 tbl_retargetting_c2l and tbl_coc_nonpaid_data"
#sudo rm -fv /tmp/tbl_retargetting_c2l_${DATE}${slot}.txt /tmp/tbl_coc_nonpaid_data_${DATE}${slot}.txt
#ERROR
#
#echo "Removing txt on 11.151 tbl_retargetting_c2l"
#ssh $SSH_ARG web_backup@192.168.11.151 "sudo rm -fv /tmp/tbl_retargetting_c2l_${DATE}${slot}.txt"
#ERROR
##ssh $SSH_ARG web_backup@192.168.12.218 "sudo rm -fv /tmp/tbl_retargetting_c2l_${DATE}${slot}.txt"
##ERROR
#
#echo "Removing txt on 1.8 tbl_coc_nonpaid_data"
#ssh $SSH_ARG web_backup@192.168.1.8 "sudo rm -fv /tmp/tbl_coc_nonpaid_data_${DATE}${slot}.txt"
#ERROR

SUCCESS

