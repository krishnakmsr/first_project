#!/bin/bash

DEFAULT_INTERFACE=$(/sbin/route -n | awk '{ print $1,$8 }' | grep "0.0.0.0" | cut -d " " -f 2)
SERIP=$(/sbin/ifconfig $DEFAULT_INTERFACE | grep "inet" | awk '{ print $2 }' | cut -d ":" -f 2 | head -n1)

INFO="
# Script by : Bhvaesh Prajapati
# Created from scratch on : 07-09-2021  Track Ticket No: #XXXXXXX
# Technical stake holder : Anurag Singh 
# Contact no of technical stake holder : Anurag Singh => XXXX
# Business stake holder : Yogesh Verma
# Purpose / Description : tbl_searches_category_monthly_cnt transfer to 0.30  
#----------------------------------------------------
# Last modified on : 07-09-2021
# Last modified by : Bhavesh
# Modified details : Fresh Created Script
# Script Name      : $0
# Script Running on: $SERIP
"

#-------- Variables ------------#

SSH_ARGS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
EMAIL_ID_ERR="anurag.singh9@justdial.com,yogesh.verma2@justdial.com,shravan.katti@justdial.com,abhishek.veeravelli@justdial.com,krishna.kasarla@justdial.com"
EMAIL_TO="anurag.singh9@justdial.com"
#EMAIL_CC="dbteam_mis@justdial.com"
EMAIL_CC="anurag.singh9@justdial.com"
EMAIL_SENDER="anurag.singh9@justdial.com"
SUBJECT="TBL Searches Category Monthly CNT"
LOCALUSER="process_auto"
LOCAL_MYSQL_USER="process_auto"
LOCAL_MYSQL_PASS="gr8exp9master"
HOST="localhost"
LOCALPASSWD="gr8exp9master"
date=`date +%d-%m-%Y`
REM_SRV="172.29.0.30"
HNAME="192.168.12.25"
SPDATE=$(date +%Y-%m-%d -d "3days ago")
#SPDATE="2021-08-31"  #### Date format = YYYY-MM-DD Example= 2021-09-04


 # ------ Function Define ------ #
function CHECK () {
if [ $? -eq 0 ]; then
                echo "${STEP} is Successfully Completed"
else
                echo "Failed"
        echo -e "${SUBJECT} is failed in step ${STEP}\n\n\n ${INFO}" | mail  -s "`echo -e \"${SUBJECT} failed\nContent-Type: text/plain\"`" ${EMAIL_ID_ERR} -a '/var/log/daily_log/tbl_searches_category_monthly_cnt.log'
                /bin/bash /scripts/standard_sms_sending_script.sh -m "${SUBJECT} is Failed on 192.168.12.25" -f /scripts/tbl_searches_category_monthly_cnt.txt
                exit 1
fi
}

TIMESTAMP(){
date +"%Y-%m-%d %T"
}

SMALL_LINE(){
echo -e  "\n---------------------------------\n"
}

MED_LINE(){
echo -e  "\n====================================================================================\n"
}

BIG_LINE(){
echo -e  "\n####################################################################################\n"
}

PORT_3006='--socket=/var/lib/SQL/mysql/mysql.sock'
PORT_3007='--socket=/var/lib/SQL_07/mysql/mysql.sock'


############ Starting Main pricess ##########################
BIG_LINE


#----------------------------------------------------------------
STEP="==>0.1"

echo "Cleanup old files from Server if present From ${HNAME}"
rm -fv /tmp/tbl_searches_category_monthly_cnt.txt /tmp/tbl_searches_company_monthly_cnt.txt

CHECK

MED_LINE

#----------------------------------------------------------------
STEP="==>0.2"
echo "Cleanup old files from Server if present From $REM_SRV"
rm -fv /tmp/tbl_searches_category_monthly_cnt.txt /tmp/tbl_searches_company_monthly_cnt.txt

CHECK

MED_LINE

#------------------------------------------------------------

### Main process 

#----------------------------------------------------------------
STEP="==>1.0" 
echo "${STEP} is start at `date` "
echo "Executing SP search_input.sp_searches_category_company_monthly_cnts"
sudo mysql -Bse "CALL search_input.sp_searches_category_company_monthly_cnts('$SPDATE');"
CHECK

MED_LINE

#----------------------------------------------------------------

STEP="==>2.0"
echo "Trasferring CSV files from $HOST to $REM_SRV"
scp ${SSH_ARGS} /tmp/tbl_searches_category_monthly_cnt.txt web_backup@${REM_SRV}:/tmp/
CHECK

scp ${SSH_ARGS} /tmp/tbl_searches_company_monthly_cnt.txt web_backup@${REM_SRV}:/tmp/
CHECK

MED_LINE

#----------------------------------------------------------------

STEP="==>3.0"
echo "Changing permission of CSV files on $REM_SRV"
ssh ${SSH_ARGS} web_backup@${REM_SRV} "sudo chown mysql.mysql /tmp/tbl_searches_company_monthly_cnt.txt /tmp/tbl_searches_category_monthly_cnt.txt"
CHECK

MED_LINE


#----------------------------------------------------------------

STEP="==>3.1"
echo "Copy file to mysql temp file on $REM_SRV"
ssh ${SSH_ARGS} web_backup@${REM_SRV} "sudo rm -fv /var/lib/mysql-files/tbl_searches_company_monthly_cnt.txt && sudo rm -fv /var/lib/mysql-files/tbl_searches_category_monthly_cnt.txt && sudo cp -arvpP /tmp/tbl_searches_company_monthly_cnt.txt /var/lib/mysql-files/ && sudo cp -arvpP /tmp/tbl_searches_category_monthly_cnt.txt /var/lib/mysql-files/"
CHECK

MED_LINE

#----------------------------------------------------------------

STEP="==>3.2"
echo "Preparing load data query on $REM_SRV for category_monthly_cnt"
ssh ${SSH_ARGS} web_backup@${REM_SRV} "sudo rm -fv /var/lib/mysql-files/query_category && sudo cp /scripts/query_category /var/lib/mysql-files/query_category && sudo chown mysql.mysql /var/lib/mysql-files/query_category"
CHECK

MED_LINE

#----------------------------------------------------------------

STEP="==>3.3"
echo "Executing load data query on $REM_SRV for category_monthly_cnt "
ssh ${SSH_ARGS} web_backup@${REM_SRV} "sudo mysql -Bse 'source /var/lib/mysql-files/query_category;'"
CHECK

MED_LINE


#----------------------------------------------------------------

STEP="==>3.4"
echo "Preparing load data query on $REM_SRV for company_monthly_cnt"
ssh ${SSH_ARGS} web_backup@${REM_SRV} "sudo rm -fv /var/lib/mysql-files/query_company && sudo cp /scripts/query_company /var/lib/mysql-files/query_company && sudo chown mysql.mysql /var/lib/mysql-files/query_company"
CHECK

MED_LINE

#----------------------------------------------------------------

STEP="==>3.5"
echo "Executing load data query on $REM_SRV for company_monthly_cnt "
ssh ${SSH_ARGS} web_backup@${REM_SRV} "sudo mysql -Bse 'source /var/lib/mysql-files/query_company'"
CHECK

MED_LINE


#----------------------------------------------------------------

STEP="==>4.0"

echo "${STEP} is start at `date` "
echo "Calling SP search_input.sp_searches_category_company_monthly_cnts('$SPDATE') on $REM_SRV."
ssh ${SSH_ARGS} web_backup@${REM_SRV} "sudo mysql -Bse \"CALL search_input.sp_searches_category_company_monthly_cnts('$SPDATE');\""
CHECK

MED_LINE

#-------------------------------------------------------------

STEP="==>5.0"

echo "--------------------- Script is Completed Sending Success email ---------------------------"

echo "Process Successfully completed on $SERIP"

echo "Sending Success Mail"

echo -e "Data for VN Daily Report process is completed successfully on ${SERIP} on '$date'\n\n\n $INFO" | mail -s "${SUBJECT} Success" -r ${EMAIL_SENDER} -c ${EMAIL_CC} ${EMAIL_TO}

echo "--------> All Process Completed Successfully At $(TIMESTAMP) <--------"
exit 0

