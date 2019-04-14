#!/bin/bash

# Confusion matrix
# Flight will be canceld only when ( departure delay >= 15 min ) AND ( arrival delay >= 15 min )
# Canceling the meeting correctly will be defined as *positive*
#
#|                                                       | arrival delay < 15 min       | arrival delay >= 15 min       |
#| Do not cancel the meeting (departure delay < 15 min ) | (a) Correct ( true negative) | (c) False negative            | 
#| Cancel the meeting        (departure delay >= 15 min) | (b) False positive           | (d) Correct ( true positive ) |
#

arr_threshold=15
dep_threshold=15
user=root
password=YuTennis123

mysql_host=`gcloud sql instances describe flights --format='value(ipAddresses.ipAddress)'`
echo "MySQL host is $mysql_host"

#
# Query (a)
#
echo "Executing query (a)"
echo "select count(dest) from flights where dep_delay < $dep_threshold AND arr_delay < $arr_threshold" \
  | mysql --host=$mysql_host --user=$user --password=$password --database bts

#
# Query (b)
#
echo "Executing query (b)"
echo "select count(dest) from flights where dep_delay >= $dep_threshold AND arr_delay < $arr_threshold" \
  | mysql --host=$mysql_host --user=$user --password=$password --database bts

#
# Query (c)
#
echo "Executing query (c)"
echo "select count(dest) from flights where dep_delay < $dep_threshold AND arr_delay >= $arr_threshold" \
  | mysql --host=$mysql_host --user=$user --password=$password --database bts

#
# Query (d)
#
echo "Executing query (d)"
echo "select count(dest) from flights where dep_delay >= $dep_threshold AND arr_delay >= $arr_threshold" \
  | mysql --host=$mysql_host --user=$user --password=$password --database bts
