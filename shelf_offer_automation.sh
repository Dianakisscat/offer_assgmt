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

echo " Part 1 Input Tables"
cust_item_level_ranking=dy_cust_level_ranking_150
final_offer_bank_table=msn_rd_offer_bank_table_20160802
final_vendor_offer_table=msn_rd_vendor_offer_table_20160802
product_table=msn_rd_product_rs3



card_hist_table=msn_card_hist
Marketing_customer_table=msn_rd_customer_rs3_20160621
production_customer_table=msn_member
neo_segment_table=mrsn_cust_seg_neo_20160513_add
strt_dt=2015-06-13
end_dt=2016-06-12
cust_tlog_table=msn_rd_cust_tlog_rs3_20160621
tlog_table=msn_rd_tlog_rs3_20160621
offer_bank_hist=msn_campaign_offer_Bank_hist
Offer_bank_supergroups=msn_campaign_offer_bank_groups
vendor_sku_hist=mor_vmps_sku_hist
vendor_form_hist=mor_vmps_vendor_form_hist
vendor_offer_hist=mor_vmps_offer_hist
vendor_campaign_hist=mor_vmps_campaign_hist







cohort=1
export cohort
while [ $cohort -le 30 ]
do



echo
echo "Creating individual tables for average spend per offer bank in the cohort "$cohort
echo

#nzsql -host 10.231.146.160 -port 5480 -d pprcmmrn01 -u $user -pw $pass -v user=$user -v cohort=$cohort -v product_table=$product_table -v final_offer_bank_table=$final_offer_bank_table -v card_hist_table=$card_hist_table -v strt_dt=\'${strt_dt}\' -v end_dt=\'${end_dt}\' -v cust_tlog_table=$cust_tlog_table -v tlog_table=$tlog_table  -f Average_txn_ofb_spend.sql > Average_txn_ofb_spend_$cohort.log 2>&1


#nzsql -host 10.231.146.160 -port 5480 -d pprcmmrn01 -u $user -pw $pass -v cust_item_level_ranking=$cust_item_level_ranking -v cust_ofb_level_ranking=$cust_ofb_level_ranking -v cohort=$cohort -v user=$user -v final_vendor_offer_table=$final_vendor_offer_table -v final_offer_bank_table=$final_offer_bank_table -v product_table=$product_table -f offer_assignment_DM1_part1.sql > offer_assignment_DM1_part1_$cohort.log 2>&1

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
