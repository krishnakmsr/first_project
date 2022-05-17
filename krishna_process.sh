#!/bin/bash
#AUTHOR -Nepolian

SSH_ARG='-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q'
MAILS="krishna.kasarla@justdial.com"
#MAILS="krishna.kasarla@justdial.com"
DATE=`date +%d%m%y`                                                                      ###Today's date 
ip_date=`date +%d%b%y -d " 1 day ago" | tr '[:upper:]' '[:lower:]'`                ###Yesterday date SP format  


ERROR(){
        if [ $? -ne 0 ]; 
        then 
            echo "process $SUBJECT failed $DATE"
            echo "vn_regular_dynamic failed on 11.149" | mail -s "vn_regular_dynamic Fsiled" ${MAILS}
            exit 1 ;
        fi
}

####STEP1:SP CALL ON 192.168.12.25#### ##############

ssh $SSH_ARG web_backup@192.168.12.25 "sudo mysql -e 'call search_input.sp_jdmart_report_searches_install_hotkey('${ip_date}')'"
ERROR

####STEP2:TABLE TRANSFER FROM 192.168.12.25#### ##############
ssh $SSH_ARG web_backup@192.168.12.25 "sudo mysqldump  test tbl_cummulative_installs_jdmart  | gzip -f > /tmp/tbl_cummulative_installs_jdmart.sql.gz "
ERROR
ssh $SSH_ARG web_backup@192.168.12.25 "sudo mysqldump  test tbl_jdmart_install_uninstall_devices_final_report  | gzip -f > /tmp/tbl_jdmart_install_uninstall_devices_final_report.sql.gz "
ERROR
ssh $SSH_ARG web_backup@192.168.12.25 "sudo mysqldump  test tbl_jdmart_searches_report  | gzip -f > /tmp/tbl_jdmart_searches_report.sql.gz "
ERROR
ssh $SSH_ARG web_backup@192.168.12.25 "sudo mysqldump  test tbl_jdmart_searches_report1  | gzip -f > /tmp/tbl_jdmart_searches_report1.sql.gz "
ERROR
ssh $SSH_ARG web_backup@192.168.12.25 "sudo mysqldump  test tbl_jdmart_hotkey_clicks_report| gzip -f > /tmp/tbl_jdmart_hotkey_clicks_report.sql.gz "
ERROR

###############################TRANSFERRING FILE FROM 192.168.12.25 to 192.168.11.149 #######################################################

ssh $SSH_ARG web_backup@192.168.12.25 "sudo scp /tmp/tbl_cummulative_installs_jdmart.sql.gz /tmp/tbl_jdmart_install_uninstall_devices_final_report.sql.gz  /tmp/tbl_jdmart_searches_report.sql.gz /tmp/tbl_jdmart_searches_report1.sql.gz /tmp/tbl_jdmart_hotkey_clicks_report.sql.gz web_backup@192.168.11.149:/tmp"
ERROR
###############################UNZIPPING AND RESTORING ##################################################################


ssh  $SSH_ARG web_backup@192.168.11.149  "gunzip -f /tmp/tbl_cummulative_installs_jdmart.sql.gz /tmp/tbl_jdmart_install_uninstall_devices_final_report.sql.gz  /tmp/tbl_jdmart_searches_report.sql.gz /tmp/tbl_jdmart_searches_report1.sql.gz /tmp/tbl_jdmart_hotkey_clicks_report.sql.gz"
ERROR
sudo mysql test < /tmp/tbl_cummulative_installs_jdmart.sql
ERROR
sudo mysql test < /tmp/tbl_jdmart_install_uninstall_devices_final_report.sql
ERROR
sudo mysql test < /tmp/tbl_jdmart_searches_report.sql
ERROR
sudo mysql test < /tmp/tbl_jdmart_searches_report1.sql
ERROR
sudo mysql test < /tmp/tbl_jdmart_hotkey_clicks_report.sql
ERROR

####STEP3:CALLING SP -call test.sp_jdmart_rating_review on 192.168.12.36 ###############

ssh  $SSH_ARG web_backup@192.168.1.8 "sudo mysql -Bse 'call test.sp_jdmart_report_visits('${ip_date}');'"
ERROR

####################STEP4: TABLE TRANSFER FROM 192.168.1.8 to 192.168.11.149#####################################################################
ssh $SSH_ARG web_backup@192.168.1.8 "sudo mysqldump  test  tbl_visits_data_mobile | gzip -f > /tmp/tbl_visits_data_mobile.sql.gz "
ERROR
ssh $SSH_ARG web_backup@192.168.1.8 "sudo mysqldump  test  tbl_visits_data_website | gzip -f > /tmp/tbl_visits_data_website.sql.gz "
ERROR

ssh $SSH_ARG web_backup@192.168.1.8 "scp /tmp/tbl_visits_data_mobile.sql.gz /tmp/tbl_visits_data_website.sql.gz  web_backup@192.168.11.149:/tmp "
ERROR

ssh $SSH_ARG web_backup@192.168.11.149 "gunzip -fv /tmp/tbl_visits_data_mobile.sql.gz /tmp/tbl_visits_data_website.sql.gz "
ERROR

sudo mysql test < /tmp/tbl_visits_data_mobile.sql
ERROR

sudo mysql test < /tmp/tbl_visits_data_website.sql
ERROR
####################################STEP5: call test.sp_jdmart_rating_review(ip_date) from 192.168.12.36 ##################


ssh $SSH_ARG web_backup@192.168.12.36  "sudo mysql -e 'call test.sp_jdmart_rating_review('${ip_date}');'"
ERROR

###################################STEP6: table transfer from 192.168.12.36 to 192.168.11.149##################################################################

ssh $SSH_ARG web_backup@192.168.12.36 "sudo mysqldump  test  tmp_jdmart_review_cnt | gzip -f > /tmp/tmp_jdmart_review_cnt.sql.gz "
ERROR

ssh $SSH_ARG web_backup@192.168.12.36 "scp  /tmp/tmp_jdmart_review_cnt.sql.gz web_backup@192.168.11.149:/tmp "
ERROR

ssh $SSH_ARG web_backup@192.168.11.149 "gunzip -fv /tmp/tmp_jdmart_review_cnt.sql.gz"
ERROR

sudo mysql test < /tmp/tmp_jdmart_review_cnt.sql
ERROR

#####################################STEP7: QUERY EXECUTION ON 192.168.12.219############################################################################

ssh $SSH_ARG web_backup@192.168.12.219 "sudo mysql -Bse 'DROP TABLE IF EXISTS test.tbl_jdmart_active_listing_cnts;'"
ERROR

ssh $SSH_ARG web_backup@192.168.12.219 "sudo mysql -Bse 'CREATE TABLE test.tbl_jdmart_active_listing_cnts SELECT COUNT(1) active_listing,SUM(IF(paid=1,1,0)) active_paid_listing FROM db_reports.tbl_jdmart_eligible_docids;'"
ERROR


##################################STEP8: TABLE TRANSFER FROM 192.168.12.219 to 192.168.11.149(#######################################################################

ssh $SSH_ARG web_backup@192.168.12.219 "sudo mysqldump  test  tbl_jdmart_active_listing_cnts | gzip -f > /tmp/tbl_jdmart_active_listing_cnts.sql.gz"
ERROR

ssh $SSH_ARG web_backup@192.168.12.219 "sudo scp /tmp/tbl_jdmart_active_listing_cnts.sql.gz web_backup@192.168.11.149:/tmp"
ERROR

ssh $SSH_ARG web_backup@192.168.11.149 "gunzip -fv /tmp/tbl_jdmart_active_listing_cnts.sql.gz"
ERROR

sudo mysql test < /tmp/tbl_jdmart_active_listing_cnts.sql
ERROR

#######################################STEP9: call search_input.sp_jdmart_report(ip_date) on 192.168.11.149################################################################################################

mysql -e "call search_input.sp_jdmart_report('${ip_date}'') ;"
ERROR

####################SUCCESS MESSAGE############################
SUCCESS
echo " Krishna_Process success on 11.149" | mail -s "Process Krishna_Process   successfully completed for $PREVIOUS_DATE" ${MAILS}

