
------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------
drop table Vendor_custs_part1;
create temp table Vendor_custs_part1 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
offer_incentive_final_allocations_catalina_3
where mail_opt_in_ind <>0 and type='vendor') b
where a.cust_acct_key=b.cust_acct_key;



drop table non_vendor_custs_part2;
create temp table non_vendor_custs_part2 as
select a.*
from
offer_incentive_final_allocations_catalina_3 a
left join
(select distinct cust_acct_key from 
offer_incentive_final_allocations_catalina_3
where mail_opt_in_ind <>0 and type='vendor') b
on a.cust_acct_key=b.cust_acct_key
where b.cust_acct_key is null
and a.mail_opt_in_ind<>0;

create temp table non_vendor_custs_part3 as
select a.cust_acct_key,b.card_nbr_hash
from
(select distinct cust_acct_key from non_vendor_custs_part2) a,
msn_card_hist b
where a.cust_acct_key=b.cust_acct_key
group by 1,2;


drop table non_vendor_custs_part4;
create temp table non_vendor_custs_part4 as
select a.tran_id,b.card_nbr_hash,b.cust_acct_key
from
:cust_tlog_table a,
non_vendor_custs_part3 b
where a.acct_id=b.card_nbr_hash
and a.tran_dt between '2015-06-13' and '2016-06-12'
group by 1,2,3;


drop table non_vendor_custs_part5;
create temp table non_vendor_custs_part5 as
select b.tran_id,b.card_nbr_hash,b.cust_acct_key,a.prod_id,case when extract(dow from a.tran_dt) = 1 then a.tran_dt 
            else a.tran_dt + 7 - extract(dow from a.tran_dt) +1
              end as WeekEnding,sum(a.sale_amt) as sales
from
:tlog_table a,
non_vendor_custs_part4 b
where a.tran_id=b.tran_id
group by 1,2,3,4,5;

drop table non_vendor_custs_part6;
create temp table non_vendor_custs_part6 as
select a.*,b.prod_hier_id_lvl1,b.prod_hier_id_lvl2,b.prod_hier_id_lvl3,b.prod_hier_id_lvl4,b.prod_hier_id_lvl5
from 
non_vendor_custs_part5 a,
:product_table b
where a.prod_id=b.prod_id;


\echo
\echo ' Summary Time'
\echo

drop table non_vendor_custs_part7;
create table non_vendor_custs_part7 as
select a.*,b.depth_category,(1.0*total_sales/txns) as avg_txn_size
from
(select cust_acct_key, count(distinct tran_id) as txns,count(distinct weekending) as weeks_shopped,count(distinct prod_hier_id_lvl3) as breadth_categories,sum(sales) as total_sales
from 
non_vendor_custs_part6
group by 1) a
left join
(select cust_acct_key,avg(prods) as depth_category
from
(select cust_acct_key,prod_hier_id_lvl3,count(distinct prod_id) as prods
from
non_vendor_custs_part6
group by 1,2) a
group by 1) b
on a.cust_acct_key=b.cust_acct_key;

drop table non_vendor_custs_part8;
create table non_vendor_custs_part8 as
select *,100.0*txns/avg_txns as index_txns,
100.0*weeks_shopped/avgs_weeks_shopped as index_weeks_shopped,
100.0*breadth_categories/avg_breadth as index_breadth,
100.0*depth_category/avg_depth as index_depth,
100.0*total_sales/avg_total_sales as index_sales,
100.0*avg_txn_size/overall_avg_txn_size as index_avg_txn_size,
(index_txns+index_weeks_shopped+index_breadth+index_depth+index_sales+index_avg_txn_size)/600 as avg_index,
rank() over (order by avg_index desc) as rank_cust
from
non_vendor_custs_part7 a,
(select 
avg(txns) as avg_txns,
avg(weeks_shopped) as avgs_weeks_shopped,
avg(breadth_categories) as avg_breadth,
avg(depth_category) as avg_depth,
avg(total_sales) as avg_total_sales,
avg(avg_txn_size) as overall_avg_txn_size
from 
non_vendor_custs_part7) b
order by rank_cust;



create temp table non_vendor_custs_part7_temp as
	select a.*, b.partition_new 
	from 
	non_vendor_custs_part7 a
	left join
	dy_partition_new b
	on a.cust_acct_key=b.cust_acct_key
	where b.cust_acct_key is not null


--get summary data
select ...... union
select partition_new, avg(txns)/avg(weeks_shopped) as freq,avg(total_sales) as avg_sales, avg(breadth_categories) as breadth,  avg(avg_txn_size) as trx_size from non_vendor_custs_part7_temp group by 1;


/*
drop table non_Vendor_custs_part9;
create temp table non_Vendor_custs_part9 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
non_vendor_custs_part8
where rank_cust<200000) b
where a.cust_acct_key=b.cust_acct_key;


drop table non_Vendor_custs_part10;
create temp table non_Vendor_custs_part10 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
non_vendor_custs_part8
where rank_cust>199999 and rank_cust<400000) b
where a.cust_acct_key=b.cust_acct_key;


drop table non_Vendor_custs_part11;
create temp table non_Vendor_custs_part11 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
non_vendor_custs_part8
where rank_cust>399999 and rank_cust<600000) b
where a.cust_acct_key=b.cust_acct_key;

drop table non_Vendor_custs_part12;
create temp table non_Vendor_custs_part12 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
non_vendor_custs_part8
where rank_cust>599999 and rank_cust<800000) b
where a.cust_acct_key=b.cust_acct_key;

drop table non_Vendor_custs_part13;
create temp table non_Vendor_custs_part13 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
non_vendor_custs_part8
where rank_cust>799999 and rank_cust<1000000) b
where a.cust_acct_key=b.cust_acct_key;

drop table non_Vendor_custs_part14;
create temp table non_Vendor_custs_part14 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
non_vendor_custs_part8
where rank_cust>999999 and rank_cust<1200000) b
where a.cust_acct_key=b.cust_acct_key;

drop table non_Vendor_custs_part15;
create temp table non_Vendor_custs_part15 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
non_vendor_custs_part8
where rank_cust>1199999 and rank_cust<1400000) b
where a.cust_acct_key=b.cust_acct_key;

drop table non_Vendor_custs_part16;
create temp table non_Vendor_custs_part16 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
non_vendor_custs_part8
where rank_cust>1399999 and rank_cust<1600000) b
where a.cust_acct_key=b.cust_acct_key;
*/


drop table non_Vendor_custs_part9;
create temp table non_Vendor_custs_part9 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
dy_partition_new where partition_new=1) b
where a.cust_acct_key=b.cust_acct_key;


drop table non_Vendor_custs_part10;
create temp table non_Vendor_custs_part10 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from
dy_partition_new where partition_new=2) b
where a.cust_acct_key=b.cust_acct_key;


drop table non_Vendor_custs_part11;
create temp table non_Vendor_custs_part11 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
dy_partition_new where partition_new=3) b
where a.cust_acct_key=b.cust_acct_key;

drop table non_Vendor_custs_part12;
create temp table non_Vendor_custs_part12 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
dy_partition_new where partition_new=4) b
where a.cust_acct_key=b.cust_acct_key;

drop table non_Vendor_custs_part13;
create temp table non_Vendor_custs_part13 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
dy_partition_new where partition_new=5) b
where a.cust_acct_key=b.cust_acct_key;

drop table non_Vendor_custs_part14;
create temp table non_Vendor_custs_part14 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
dy_partition_new where partition_new=6) b
where a.cust_acct_key=b.cust_acct_key;

drop table non_Vendor_custs_part15;
create temp table non_Vendor_custs_part15 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
dy_partition_new where partition_new=7) b
where a.cust_acct_key=b.cust_acct_key;

drop table non_Vendor_custs_part16;
create temp table non_Vendor_custs_part16 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
dy_partition_new where partition_new=8) b
where a.cust_acct_key=b.cust_acct_key;

drop table non_Vendor_custs_part17;
create temp table non_Vendor_custs_part17 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
dy_partition_new where partition_new=9) b
where a.cust_acct_key=b.cust_acct_key;

drop table non_Vendor_custs_part18;
create temp table non_Vendor_custs_part18 as
select a.* 
from
offer_incentive_final_allocations_catalina_3 a,
(select distinct cust_acct_key from 
dy_partition_new where partition_new=10) b
where a.cust_acct_key=b.cust_acct_key;








/*
select 1 as priority_custs,'vendor_custs' as cust_category,cust_acct_key from Vendor_custs_part1
union
select 2 as priority_custs,'first_200k' as cust_category,cust_acct_key from non_Vendor_custs_part9
union
select 3 as priority_custs,'second_200k' as cust_category,cust_acct_key from non_Vendor_custs_part10
union
select 4 as priority_custs,'third_200k' as cust_category,cust_acct_key from non_Vendor_custs_part11
union
select 5 as priority_custs,'fourth_200k' as cust_category,cust_acct_key from non_Vendor_custs_part12
union
select 6 as priority_custs,'fifth_200k' as cust_category,cust_acct_key from non_Vendor_custs_part13
union
select 7 as priority_custs,'sixth_200k' as cust_category,cust_acct_key from non_Vendor_custs_part14
union
select 8 as priority_custs,'seventh_200k' as cust_category,cust_acct_key from non_Vendor_custs_part15
union
select 9 as priority_custs,'eighth_200k' as cust_category,cust_acct_key from non_Vendor_custs_part16
*/



--test
--test



select *
from
(select 0 as priority_custs,'vendor_custs' as cust_category,type,case when type='vendor' then 0 when type='product' then 0.7 when type='ofb' then 0.7 else 1 end as resp_rate,count(distinct account_number) as custs,sum(offers) as offers,sum(rebate) as total_rebate,total_rebate*resp_rate as cost_redemption
from
(select account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from 
Vendor_custs_part1 
where mail_opt_in_ind <> 0
group by 1,2) a
group by 1,2,3,4

union

select 1 as priority_custs,'first_160k' as cust_category,type,case when type='vendor' then 0 when type='product' then 0.7 when type='ofb' then 0.7 else 1 end as resp_rate,count(distinct account_number) as custs,sum(offers) as offers,sum(rebate) as total_rebate,total_rebate*resp_rate as cost_redemption
from
(select account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from 
non_Vendor_custs_part9 
where mail_opt_in_ind <> 0
group by 1,2) a
group by 1,2,3,4

union

select 2 as priority_custs,'second_160k' as cust_category,type,case when type='vendor' then 0 when type='product' then 0.7 when type='ofb' then 0.7 else 1 end as resp_rate,count(distinct account_number) as custs,sum(offers) as offers,sum(rebate) as total_rebate,total_rebate*resp_rate as cost_redemption
from
(select account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from 
non_Vendor_custs_part10 
where mail_opt_in_ind <> 0
group by 1,2) a
group by 1,2,3,4

union

select 3 as priority_custs,'third_160k' as cust_category,type,case when type='vendor' then 0 when type='product' then 0.7 when type='ofb' then 0.7 else 1 end as resp_rate,count(distinct account_number) as custs,sum(offers) as offers,sum(rebate) as total_rebate,total_rebate*resp_rate as cost_redemption
from
(select account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from 
non_Vendor_custs_part11 
where mail_opt_in_ind <> 0
group by 1,2) a
group by 1,2,3,4

union

select 4 as priority_custs,'fourth_160k' as cust_category,type,case when type='vendor' then 0 when type='product' then 0.7 when type='ofb' then 0.7 else 1 end as resp_rate,count(distinct account_number) as custs,sum(offers) as offers,sum(rebate) as total_rebate,total_rebate*resp_rate as cost_redemption
from
(select account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from 
non_Vendor_custs_part12 
where mail_opt_in_ind <> 0
group by 1,2) a
group by 1,2,3,4

union

select 5 as priority_custs,'fifth_160k' as cust_category,type,case when type='vendor' then 0 when type='product' then 0.7 when type='ofb' then 0.7 else 1 end as resp_rate,count(distinct account_number) as custs,sum(offers) as offers,sum(rebate) as total_rebate,total_rebate*resp_rate as cost_redemption
from
(select account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from 
non_Vendor_custs_part13 
where mail_opt_in_ind <> 0
group by 1,2) a
group by 1,2,3,4

union

select 6 as priority_custs,'sixth_160k' as cust_category,type,case when type='vendor' then 0 when type='product' then 0.7 when type='ofb' then 0.7 else 1 end as resp_rate,count(distinct account_number) as custs,sum(offers) as offers,sum(rebate) as total_rebate,total_rebate*resp_rate as cost_redemption
from
(select account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from 
non_Vendor_custs_part14 
where mail_opt_in_ind <> 0
group by 1,2) a
group by 1,2,3,4

union

select 7 as priority_custs,'seventh_160k' as cust_category,type,case when type='vendor' then 0 when type='product' then 0.7 when type='ofb' then 0.7 else 1 end as resp_rate,count(distinct account_number) as custs,sum(offers) as offers,sum(rebate) as total_rebate,total_rebate*resp_rate as cost_redemption
from
(select account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from 
non_Vendor_custs_part15 
where mail_opt_in_ind <> 0
group by 1,2) a
group by 1,2,3,4

union

select 8 as priority_custs,'eighth_160k' as cust_category,type,case when type='vendor' then 0 when type='product' then 0.7 when type='ofb' then 0.7 else 1 end as resp_rate,count(distinct account_number) as custs,sum(offers) as offers,sum(rebate) as total_rebate,total_rebate*resp_rate as cost_redemption
from
(select account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from 
non_Vendor_custs_part16 
where mail_opt_in_ind <> 0
group by 1,2) a
group by 1,2,3,4

union

select 9 as priority_custs,'ninth_160k' as cust_category,type,case when type='vendor' then 0 when type='product' then 0.7 when type='ofb' then 0.7 else 1 end as resp_rate,count(distinct account_number) as custs,sum(offers) as offers,sum(rebate) as total_rebate,total_rebate*resp_rate as cost_redemption
from
(select account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from 
non_Vendor_custs_part17 
where mail_opt_in_ind <> 0
group by 1,2) a
group by 1,2,3,4

union

select 10 as priority_custs,'tenth_160k' as cust_category,type,case when type='vendor' then 0 when type='product' then 0.7 when type='ofb' then 0.7 else 1 end as resp_rate,count(distinct account_number) as custs,sum(offers) as offers,sum(rebate) as total_rebate,total_rebate*resp_rate as cost_redemption
from
(select account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from 
non_Vendor_custs_part18 
where mail_opt_in_ind <> 0
group by 1,2) a
group by 1,2,3,4

) a
order by 1,3;





