#Korne/bourne/c -shebang
#!/bin/ksh
#script to create Hive partition load
#bash Hive_shell.sh /home/hduser/hivepart txn

echo "$0 is starting"
# "$0" is placeholder used display name of the script 
#name of the script ="Hive_shell.sh"

if [ $# -ne 2 ]
# "$#" is placeholder used get the no of args
#Using condition check that we are passing two argments 
#first argment is "/home/hduser/hivepart"----Source path
#Secound argment is table name
then
echo "$0 requires source path and the target table name to load , usage 
: bash Hive_shell.sh /home/hduser/hivepart txn"
exit  10
fi 

echo "$1 is the path"
echo "$2 is the tablename"

#Preparation of some data
#cp /home/hduser/hive/data/txns /home/hduser/hivepart/txns_20210101_PADE
#cp /home/hduser/hive/data/txns /home/hduser/hivepart/txns_20210101_NY
#cp /home/hduser/hive/data/txns /home/hduser/hivepart/txns_20210101_NJ
#cp /home/hduser/hive/data/txns /home/hduser/hivepart/txns_20210102_PADE
#cp /home/hduser/hive/data/txns /home/hduser/hivepart/txns_20210102_NY
#cp /home/hduser/hive/data/txns /home/hduser/hivepart/txns_20210102_NJ

#cd $1
if ls $1/txns_*_* &>/dev/null
#checking that format is there in path
then 
"creating the table for the first time "
hive -e "create table if not exist $2(txno int , txndate string ,custno int,amount Double ,category string,product string,city string,state string, spendby string)
partitioned by (datadt date,region string) row format delimited fields terminated by ","
stored as textfile;"
#hive -e to execute the hive queries
#above queries that creating the table with name $2 arg "txn" and partitioned based on the date and region
#with the delimiter of "," and stored it as text format.
for i in $1/txns_*_*
do 
echo "file with path name is $i"
#need take one by one file using the looping condition "$1 -/home/hduser/hivepart/txns_20210101_PADE"
filename=$(basename $i)
#basename is base path for location "txns_20210101_PADE" 
echo "$filename" #txns_20210101_PADE

#identify date from $filename "txns_20210101_PADE"----20210101---format----2020-10-07
dt=`echo $filename | awk -F'_' '{print $2}'`
dtfmt=`date -d $dt + '%Y-%m-%d'` ----format as per business standard
echo $dtfmt

reg=`echo $filename | awk -F'_' '{print $3}'`
echo reg

echo "LOAD DATA LOCAL INPATH '$1/$filename' overwrite into table $2 partition(datadt='$dtfmt',region='$reg');"

echo "show partitions $2;"

echo "loading hive table"
hive -f /home/hduser/hivepart/partload.hql 
#once data loaded then file move to achive directory 
echo "archiving the files"
mkdir -p /home/hduser/hivepartarchive/
gzip $1/txns_*_*
mv $1/txns_*_*.gz /home/hduser/hivepartarchive/
else
echo "`date` There is no files present in the given source location $1"
fi
echo "Script finished"
