 #!/bin/bash
set - xv

\echo
\echo \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
time_now=$(date)
echo Code run on $time_now
\echo \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
\echo

user=dyang
pass='Aug2016_diana'

export user

echo "Input Tables"
cust_item_level_ranking=dy_cust_level_ranking_150
final_offer_bank_table=msn_rd_offer_bank_table_20160802
final_vendor_offer_table=msn_rd_vendor_offer_table_20160802
product_table=msn_rd_product_rs3


cohort=1
export cohort
while [ $cohort -le 1 ]
do

nzsql -host 10.231.146.160 -port 5480 -d pprcmmrn01 -u $user -pw $pass \
-v user=$user \
-v cohort=$cohort  \
-v cust_item_level_ranking=$cust_item_level_ranking \
-v final_offer_bank_table=$final_offer_bank_table \
-v final_vendor_offer_table=$final_vendor_offer_table \
-v product_table=$product_table \
-f shelf_offer_assignment_part1.sql > shelf_offer_assignment_part1_$cohort.log 2>&1

j=0
i=1
threshold=1

while [ $threshold -gt 0 ]
do

#iterations here
nzsql -host 10.231.146.160 -port 5480 -d pprcmmrn01 -u $user -pw $pass -v i=$i -v j=$j -f shelf_offer_assignment_part2.sql > shelf_offer_assignment_part2_$cohort.log 2>&1

threshold=`/usr/nps/kit/bin/nzsql -host 10.231.146.160 -port 5480 -d pprcmmrn01 -u dyang -pw 'Aug2016_diana' -t -c "select count(*) from (select *, row_number()over(partition by acct_id, offer_bank_supergroup_code order by v10 desc) as super_rank from (select *, count(*)over(partition by acct_id) as total_temp from shelf_iteration_$i)a )a where super_rank > total_temp*0.5;"`  


i=$((i+1))
j=$((j+1))

done

echo Iternations needed is $j
#where to put it?
nzsql -host 10.231.146.160 -port 5480 -d pprcmmrn01 -u $user -pw $pass -v i=$i -v j=$j -v user=$user -v cohort=$cohort -f shelf_offer_assignment_part3.sql > shelf_offer_assignment_part3_$cohort.log 2>&1


cohort=$((cohort+1))
done





\echo
\echo \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
time_now=$(date)
echo Code run on $time_now
\echo \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\


echo
echo "Code Run Completed"
echo
