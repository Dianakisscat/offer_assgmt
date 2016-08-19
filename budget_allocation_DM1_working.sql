
drop table offer_incentive_final_allocations_union_all_dy;
create table offer_incentive_final_allocations_union_all_dy as
select 1 as cohort,* from offer_incentive_final_allocations1 
union
select 2 as cohort,* from offer_incentive_final_allocations2 
union
select 3 as cohort,* from offer_incentive_final_allocations3 
union
select 4 as cohort,* from offer_incentive_final_allocations4 
union
select 5 as cohort,* from offer_incentive_final_allocations5 
union
select 6 as cohort,* from offer_incentive_final_allocations6 
union
select 7 as cohort,* from offer_incentive_final_allocations7 
union
select 8 as cohort,* from offer_incentive_final_allocations8 
union
select 9 as cohort,* from offer_incentive_final_allocations9 
union
select 10 as cohort,* from offer_incentive_final_allocations10 
union
select 11 as cohort,* from offer_incentive_final_allocations11 
union
select 12 as cohort,* from offer_incentive_final_allocations12 
union
select 13 as cohort,* from offer_incentive_final_allocations13 
union
select 14 as cohort,* from offer_incentive_final_allocations14 
union
select 15 as cohort,* from offer_incentive_final_allocations15 
union
select 16 as cohort,* from offer_incentive_final_allocations16 
union
select 17 as cohort,* from offer_incentive_final_allocations17 
union
select 18 as cohort,* from offer_incentive_final_allocations18 
union
select 19 as cohort,* from offer_incentive_final_allocations19 
union
select 20 as cohort,* from offer_incentive_final_allocations20
union
select 21 as cohort,* from offer_incentive_final_allocations21
union
select 22 as cohort,* from offer_incentive_final_allocations22
union
select 23 as cohort,* from offer_incentive_final_allocations23
union
select 24 as cohort,* from offer_incentive_final_allocations24
union
select 25 as cohort,* from offer_incentive_final_allocations25
union
select 26 as cohort,* from offer_incentive_final_allocations26
union
select 27 as cohort,* from offer_incentive_final_allocations27
union
select 28 as cohort,* from offer_incentive_final_allocations28
union
select 29 as cohort,* from offer_incentive_final_allocations29
union
select 30 as cohort,* from offer_incentive_final_allocations30 ;

select * from offer_incentive_final_allocations_union_all_dy;

Grant list, select on offer_incentive_final_allocations_union_all_dy to pprcmmrn01_usr_read ;


/*********************************************************************************/
/****************************ADD INCENTIVE TYPE***********************************/
/*********************************************************************************/


/**FIND BEST INCENTIVE ACCORDING TO PE TABLE**/

--when pe between (-6,-1), choose the max incentive that is SMALLER than avg_amt;   
--when pe between (-1, -0.5) OR incentive_min > avg_amt, choose incentive_min straightforwardly

/*
create temp table vertical_pe as
select * from garcia_acct_ofb_incentive_lift_new where (pe between -6 and -1);

create temp table vertical_pe_union as
select distinct acct_id,ofb_id,avg_amt,incentive_min as tmp from vertical_pe union
select distinct acct_id,ofb_id,avg_amt,incentive_1 as tmp from vertical_pe union
select distinct acct_id,ofb_id,avg_amt,incentive_2 as tmp from vertical_pe union
select distinct acct_id,ofb_id,avg_amt,incentive_3 as tmp from vertical_pe union
select distinct acct_id,ofb_id,avg_amt,incentive_max as tmp from vertical_pe;

drop table vertical_pe_max_lift;
create temp table vertical_pe_max_lift as
select distinct acct_id, ofb_id as precima_ofb_id, max(tmp) as inc_optimal
from vertical_pe_union where tmp <= avg_amt
group by 1,2;

drop table dy_garcia_acct_ofb_inc_optimal;
create table dy_garcia_acct_ofb_inc_optimal as
(select *, incentive_min as inc_optimal 
 from garcia_acct_ofb_incentive_lift_new where (pe<-0.5 and pe>-1)  or (incentive_min > avg_amt))     --pe between (-1, -0.5) OR incentive_min > avg_amt
union
(select a.*, b.inc_optimal from
vertical_pe a, vertical_pe_max_lift b															  --pe between (-6,-1)
where a.acct_id=b.precima_ofb_id
and a.ofb_id=b.ofb_id);

Grant list, select on dy_garcia_acct_ofb_inc_optimal to pprcmmrn01_usr_read ;
*/

/*first column: adjusted incentive rate*/

drop table offer_incentive_final_allocations_union_all_dy_1;
create temp table offer_incentive_final_allocations_union_all_dy_1 as
select a.*, round(b.incentive_1,2) as incentive_1,round(b.incentive_2,2) as incentive_2, round(b.incentive_3,2) as incentive_3,b.pe,b.avg_amt
from offer_incentive_final_allocations_union_all_dy a
left join (select acct_id, ofb_id, pe, avg_amt, incentive_1,incentive_2,incentive_3 from dy_garcia_acct_ofb_inc_optimal group by 1,2,3,4,5,6,7) b
on a.acct_id=b.acct_id and a.precima_ofb_id=b.ofb_id;

drop table offer_incentive_final_allocations_union_all_dy_2;
create temp table offer_incentive_final_allocations_union_all_dy_2 as
select *,
case when incentive_tmp <= incentive_min then incentive_min when incentive_tmp >= incentive_max then incentive_max when incentive_tmp is null then incentive_min else incentive_tmp end as inc_adjusted,
case when
(inc_adjusted != incentive_min and inc_adjusted!= incentive_1 and inc_adjusted!= incentive_2  and inc_adjusted != incentive_max )
-- and inc_adjusted != incentive_3 and inc_adjusted!= incentive_2  ) 
then (inc_adjusted - 0.05) end as inc_lower, inc_lower + 0.1 as inc_upper,
case when pe between -6 and -1 then inc_upper when pe between -1 and -0.5 then inc_lower end as inc_bound_adjusted,
case when inc_bound_adjusted is null then inc_adjusted else inc_bound_adjusted end as inc_bound_temp
from  offer_incentive_final_allocations_union_all_dy_1 where incentive_3 is null
union
select *,
case when incentive_tmp <= incentive_min then incentive_min when incentive_tmp >= incentive_max then incentive_max when incentive_tmp is null then incentive_min else incentive_tmp end as inc_adjusted,
case when
(inc_adjusted != incentive_min and inc_adjusted!= incentive_1 and inc_adjusted!= incentive_2 and inc_adjusted != incentive_3 and inc_adjusted != incentive_max )
--  and inc_adjusted!= incentive_2  ) 
then (inc_adjusted - 0.05) end as inc_lower, inc_lower + 0.1 as inc_upper,
case when pe between -6 and -1 then inc_upper when pe between -1 and -0.5 then inc_lower end as inc_bound_adjusted,
case when inc_bound_adjusted is null then inc_adjusted else inc_bound_adjusted end as inc_bound_temp
from  offer_incentive_final_allocations_union_all_dy_1 where incentive_3 is not null; 


drop table offer_incentive_final_allocations_union_all_dy_3;
create temp table offer_incentive_final_allocations_union_all_dy_3 as
select *, 
case when precima_ofb_id in  ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67') then inc_bound_temp/1000 else inc_bound_temp end as inc_bound_final 
from
(
select cohort,account_number,cust_acct_key,acct_id,item1,v10,rank,purch_flag,precima_ofb_id,offer_bank_group_code,offer_bank_supergroup_code,type,precimavendorid,precimaofferid,priority,channel,email_opt_in_ind,mail_opt_in_ind,phone_opt_in_ind,sms_opt_in_ind,incentive_tmp,avg_ofb_txns,disc_cap_n_collared,incentive_min,incentive_max,incentive_incrementals,incentive_final,inc_bound_temp
from offer_incentive_final_allocations_union_all_dy_2 where type='ofb'
union
select *, incentive_final as inc_bound_temp from offer_incentive_final_allocations_union_all_dy where type='vendor'
union
select *, incentive_final as inc_bound_temp from offer_incentive_final_allocations_union_all_dy where type='product'
)a;


/*second column: purely optimal rate based on PE*/

--NOTICE: THERE ARE 1124256 RECORDS WITH null inc_optimal BECAUSE CANNOT FIND THEM IN THE LOOKUP TABLE
--for the purpose of calculation, using incentive_min as replacement; 

drop table offer_incentive_final_allocations_union_all_dy_4;
create table offer_incentive_final_allocations_union_all_dy_4 as
select *,
case when inc_optimal is null then incentive_min else inc_optimal end as inc_optimal_final
from (
select *, 
case when precima_ofb_id in  ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67') then inc_optimal_temp/1000 else inc_optimal_temp end as inc_optimal
from 
(	
select a.*, b.inc_optimal as inc_optimal_temp from 
offer_incentive_final_allocations_union_all_dy_3 a
left join
dy_garcia_acct_ofb_inc_optimal b
on a.acct_id=b.acct_id and a.precima_ofb_id=b.ofb_id)a
)a;

--NOTICE: A total of of 3576 NULL incentive_final
--select count(*) from offer_incentive_final_allocations_union_all_dy where incentive_final is null

Grant list, select on offer_incentive_final_allocations_union_all_dy_4 to pprcmmrn01_usr_read;




/*************************************************/
/****************** Budget Allocation*************/
/*************************************************/



--all_custs_part7_priority  3514452 customers in total, using Neo's old table

drop table offer_incentive_final_allocations_union_all_dy_4_mail; --3,703,441
create table offer_incentive_final_allocations_union_all_dy_4_mail as
select * from offer_incentive_final_allocations_union_all_dy_4 where mail_opt_in_ind<>0;

Grant list, select on offer_incentive_final_allocations_union_all_dy_4_mail to pprcmmrn01_usr_read ;


--vendor customer offer assgmt
drop table Vendor_custs_part1;  --537,390
create temp table Vendor_custs_part1 as
select * from
offer_incentive_final_allocations_union_all_dy_4_mail 
where cust_acct_key in (select cust_acct_key from offer_incentive_final_allocations_union_all_dy_4_mail where type='vendor');


-- sampling universe: non vendor customers, without null potential spend customers

drop table sampling_universe_tmp;  --3,166,051
create temp table sampling_universe_tmp as
select distinct cust_acct_key, acct_id from offer_incentive_final_allocations_union_all_dy_4_mail
except
select distinct cust_acct_key, acct_id from Vendor_custs_part1;

drop table sampling_universe; --3,061,565
create table sampling_universe as
select a.*,b.potential_spend_segment from
sampling_universe_tmp a
left join
:customer_table b
on a.cust_acct_key=b.cust_acct_key
where potential_spend_segment is not null;



--using sas to do stratified sampling

--select count(*) from dy_sample_result_dm1; 306,161

--exclude sample customers
drop table not_sampled_universe;
create table not_sampled_universe as
select cust_acct_key,potential_spend_segment from sampling_universe
except
select cust_acct_key,potential_spend_segment from dy_sample_result_dm1;




--cust list
drop table priority_custs_list_dm1;
create table priority_custs_list_dm1 as  --
select 1 as priority_custs_tmp, 'vendor' as cust_category, cust_acct_key from (select distinct cust_acct_key from Vendor_custs_part1)a
union
select 2 as priority_custs_tmp, 'sample' as cust_category, cust_acct_key from dy_sample_result_dm1
union
select 3 as priority_custs_tmp, 'HH' as cust_category, cust_acct_key from not_sampled_universe where potential_spend_segment='HH'
union
select 4 as priority_custs_tmp, 'HM' as cust_category, cust_acct_key from not_sampled_universe where potential_spend_segment='HM'
union
select 5 as priority_custs_tmp, 'HL' as cust_category, cust_acct_key from not_sampled_universe where potential_spend_segment='HL'
union
select 6 as priority_custs_tmp, 'MH' as cust_category, cust_acct_key from not_sampled_universe where potential_spend_segment='MH'
union
select 7 as priority_custs_tmp, 'MM' as cust_category, cust_acct_key from not_sampled_universe where potential_spend_segment='MM'
union
select 8 as priority_custs_tmp, 'ML' as cust_category, cust_acct_key from not_sampled_universe where potential_spend_segment='ML'
union
select 9 as priority_custs_tmp, 'LH' as cust_category, cust_acct_key from not_sampled_universe where potential_spend_segment='LH'
union
select 10 as priority_custs_tmp, 'LM' as cust_category, cust_acct_key from not_sampled_universe where potential_spend_segment='LM'
union
select 11 as priority_custs_tmp, 'LL' as cust_category, cust_acct_key from not_sampled_universe where potential_spend_segment='LL';


drop table offer_incentive_final_allocations_union_all_dy_4_mail_2;
create temp table offer_incentive_final_allocations_union_all_dy_4_mail_2 as
select *, case when priority_custs_tmp is null then 12 else priority_custs_tmp end as priority_custs 
from
(select b.priority_custs_tmp, b.cust_category, a.* from
offer_incentive_final_allocations_union_all_dy_4_mail a
left join
priority_custs_list_dm1 b
on a.cust_acct_key=b.cust_acct_key)a;


drop table priority_custs_list_dm1_final;
create table priority_custs_list_dm1_final as
select priority_custs,cust_category,cust_acct_key from offer_incentive_final_allocations_union_all_dy_4_mail_2 group by 1,2,3;


Grant list, select on priority_custs_list_dm1_final to pprcmmrn01_usr_read;

select priority_custs,cust_category,type,
case when type='vendor' then 0 when type='product' then 0.7 when type='ofb' then 0.7 else 1 end as resp_rate,
count(distinct account_number) as custs,
sum(offers) as offers,
sum(rebate) as total_rebate,
total_rebate*resp_rate as cost_redemption
from
(select priority_custs,cust_category,account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from offer_incentive_final_allocations_union_all_dy_4_mail_2 
group by 1,2,3,4)a
group by 1,2,3,4


-------------SUMMARY

--vendor
select cust_acct_key, sum(inc_bound_final) as vendor_rebate from offer_incentive_final_allocations_union_all_dy_4_mail_2 where type='vendor' group by 1
--product
select cust_acct_key, sum(inc_bound_final) as product_rebate from offer_incentive_final_allocations_union_all_dy_4_mail_2 where type='product' group by 1
--ofb
select cust_acct_key, sum(inc_bound_final) as ofb_rebate from offer_incentive_final_allocations_union_all_dy_4_mail_2 where type='ofb' group by 1


--total

drop table ty_rebate_summary_final;
create table ty_rebate_summary_final as
select b.priority_custs, b.cust_category, a.* from
(select a.*,b.vendor_rebate,c.product_rebate,d.ofb_rebate from
(select cust_acct_key, sum(inc_bound_final) as total_rebate from offer_incentive_final_allocations_union_all_dy_4_mail_2 group by 1)a
left join
(select cust_acct_key, sum(inc_bound_final) as vendor_rebate from offer_incentive_final_allocations_union_all_dy_4_mail_2 where type='vendor' group by 1)b
on a.cust_acct_key=b.cust_acct_key
left join
(select cust_acct_key, sum(inc_bound_final) as product_rebate from offer_incentive_final_allocations_union_all_dy_4_mail_2 where type='product' group by 1)c
on a.cust_acct_key=c.cust_acct_key
left join
(select cust_acct_key, sum(inc_bound_final) as ofb_rebate from offer_incentive_final_allocations_union_all_dy_4_mail_2 where type='ofb' group by 1)d
on a.cust_acct_key=d.cust_acct_key) a
left join
priority_custs_list_dm1_final b
on a.cust_acct_key=b.cust_acct_key;

Grant list, select on ty_rebate_summary_final to pprcmmrn01_usr_read;




/*************************************************/
/*************  Final assignment decisions********/
/*************************************************/

drop table final_custs_list_dm1;
create table final_custs_list_dm1 as
select * from priority_custs_list_dm1_final where priority_custs in (1,2,3,4,5,6,7);  --1,941,351 customers

drop table final_offer_assgmt_table_dm1;
create table final_offer_assgmt_table_dm1 as
select *, case when incentive_type='Points' then round((inc_bound_final * 1000),0) else round(inc_bound_final,2) end as incentive_print
from
(select b.priority_custs,b.cust_category,a.*,c.incentive_type from 
offer_incentive_final_allocations_union_all_dy_4 a,
final_custs_list_dm1 b,
msn_campaign_offer_bank_ty_incentive c
where 
a.cust_acct_key=b.cust_acct_key
and
a.precima_ofb_id=c.precima_ofb_id)a;

--QA
select count(distinct cust_acct_key) from final_offer_assgmt_table_dm1;
select cust_acct_key, count(*) as cnt from final_offer_assgmt_table_dm1 group by 1 having cnt !=8;
--cohort
select cohort,count(distinct cust_acct_key) from final_offer_assgmt_table_dm1 group by 1;
--account_number and cust_acct_key
select count(distinct cust_acct_key) from final_offer_assgmt_table_dm1; --1941351
select count(distinct account_number) from final_offer_assgmt_table_dm1; --1941351
select cust_acct_key,count(distinct account_number) as cnt from final_offer_assgmt_table_dm1 group by 1 having cnt > 1;
select account_number,count(distinct cust_acct_key) as cnt from final_offer_assgmt_table_dm1 group by 1 having cnt > 1;
--check null columns
select * from final_offer_assgmt_table_dm1 where inc_bound_final is null
--check priority groups
select priority_custs,cust_category,count(distinct cust_acct_key) as cnt from final_offer_assgmt_table_dm1 group by 1,2;
--priority_custs	cust_category	cnt
--1					vendor		537390
--2					sample		306161
--3					HH			123917
--4					HM			224676
--5					HL			157071
--6					MH			207709
--7					MM			384427

--check different offer types
select distinct type from final_offer_assgmt_table_dm1;
select distinct type, priority from final_offer_assgmt_table_dm1;
select distinct precimaofferid from final_offer_assgmt_table_dm1 where type='product';

--check incentive range and incentive type
select * from final_offer_assgmt_table_dm1 where incentive_print > incentive_max or incentive_print < incentive_min;
select distinct incentive_type from final_offer_assgmt_table_dm1 where precima_ofb_id in ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67');
select distinct incentive_print from final_offer_assgmt_table_dm1 where precima_ofb_id in ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67');

--cheeck super group rules
select acct_id,item1,count(*) as cnt from final_offer_assgmt_table_dm1 group by 1,2 having cnt >1;
select acct_id,precima_ofb_id,count(*) as cnt from final_offer_assgmt_table_dm1 group by 1,2 having cnt >1;
select acct_id,offer_bank_group_code, count(*) as cnt from final_offer_assgmt_table_dm1 group by 1,2 having cnt >1;
select acct_id,offer_bank_supergroup_code, count(*) as cnt from final_offer_assgmt_table_dm1 group by 1,2 having cnt >2;

--random check
select *  from final_offer_assgmt_table_dm1 where cust_acct_key =178904 and item1 in ('247','2');