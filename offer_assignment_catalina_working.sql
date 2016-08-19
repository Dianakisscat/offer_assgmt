


/*************************************************/
/*****TOTAL OFFER POOL FOR ALL CUSTOMERS**********/
/*************************************************/

drop table all_custs_offer_pool;
create table all_custs_offer_pool as
select 1 as cohort,* from all_offers_union_1dyang 
union
select 2 as cohort,* from all_offers_union_2dyang 
union
select 3 as cohort,* from all_offers_union_3dyang 
union
select 4 as cohort,* from all_offers_union_4dyang 
union
select 5 as cohort,* from all_offers_union_5dyang 
union
select 6 as cohort,* from all_offers_union_6dyang 
union
select 7 as cohort,* from all_offers_union_7dyang 
union
select 8 as cohort,* from all_offers_union_8dyang 
union
select 9 as cohort,* from all_offers_union_9dyang 
union
select 10 as cohort,* from all_offers_union_10dyang 
union
select 11 as cohort,* from all_offers_union_11dyang 
union
select 12 as cohort,* from all_offers_union_12dyang 
union
select 13 as cohort,* from all_offers_union_13dyang 
union
select 14 as cohort,* from all_offers_union_14dyang 
union
select 15 as cohort,* from all_offers_union_15dyang 
union
select 16 as cohort,* from all_offers_union_16dyang 
union
select 17 as cohort,* from all_offers_union_17dyang 
union
select 18 as cohort,* from all_offers_union_18dyang
union
select 19 as cohort,* from all_offers_union_19dyang 
union
select 20 as cohort,* from all_offers_union_20dyang
union
select 21 as cohort,* from all_offers_union_21dyang
union
select 22 as cohort,* from all_offers_union_22dyang
union
select 23 as cohort,* from all_offers_union_23dyang
union
select 24 as cohort,* from all_offers_union_24dyang
union
select 25 as cohort,* from all_offers_union_25dyang
union
select 26 as cohort,* from all_offers_union_26dyang
union
select 27 as cohort,* from all_offers_union_27dyang
union
select 28 as cohort,* from all_offers_union_28dyang
union
select 29 as cohort,* from all_offers_union_29dyang
union
select 30 as cohort,* from all_offers_union_30dyang;


/*************************************************/
/*****AVG TRX SPEND AMOUNT ALL CUSTOMERS**********/
/*************************************************/
drop table avg_txn_sales_ofb_id_all;
create table avg_txn_sales_ofb_id_all as
select 1 as cohort,* from avg_txn_sales_ofb_id_1 
union
select 2 as cohort,* from avg_txn_sales_ofb_id_2 
union
select 3 as cohort,* from avg_txn_sales_ofb_id_3 
union
select 4 as cohort,* from avg_txn_sales_ofb_id_4 
union
select 5 as cohort,* from avg_txn_sales_ofb_id_5 
union
select 6 as cohort,* from avg_txn_sales_ofb_id_6 
union
select 7 as cohort,* from avg_txn_sales_ofb_id_7 
union
select 8 as cohort,* from avg_txn_sales_ofb_id_8 
union
select 9 as cohort,* from avg_txn_sales_ofb_id_9 
union
select 10 as cohort,* from avg_txn_sales_ofb_id_10 
union
select 11 as cohort,* from avg_txn_sales_ofb_id_11 
union
select 12 as cohort,* from avg_txn_sales_ofb_id_12 
union
select 13 as cohort,* from avg_txn_sales_ofb_id_13 
union
select 14 as cohort,* from avg_txn_sales_ofb_id_14 
union
select 15 as cohort,* from avg_txn_sales_ofb_id_15 
union
select 16 as cohort,* from avg_txn_sales_ofb_id_16 
union
select 17 as cohort,* from avg_txn_sales_ofb_id_17 
union
select 18 as cohort,* from avg_txn_sales_ofb_id_18 
union
select 19 as cohort,* from avg_txn_sales_ofb_id_19 
union
select 20 as cohort,* from avg_txn_sales_ofb_id_20
union
select 21 as cohort,* from avg_txn_sales_ofb_id_21
union
select 22 as cohort,* from avg_txn_sales_ofb_id_22
union
select 23 as cohort,* from avg_txn_sales_ofb_id_23
union
select 24 as cohort,* from avg_txn_sales_ofb_id_24
union
select 25 as cohort,* from avg_txn_sales_ofb_id_25
union
select 26 as cohort,* from avg_txn_sales_ofb_id_26
union
select 27 as cohort,* from avg_txn_sales_ofb_id_27
union
select 28 as cohort,* from avg_txn_sales_ofb_id_28
union
select 29 as cohort,* from avg_txn_sales_ofb_id_29
union
select 30 as cohort,* from avg_txn_sales_ofb_id_30 ;



/*************************************************/
/*****CATALINA OFFER ASSIGNMENT PROCESS***********/
/*************************************************/

--CATALINA Offer pool: excluding DM1 from the original offer pool

drop table catalina_offer_pool;
create temp table catalina_offer_pool as
select * from all_custs_offer_pool 
except 
select cohort,acct_id,item1,v10,rank,purch_flag,precima_ofb_id,offer_bank_group_code,offer_bank_supergroup_code,type,precimavendorid,precimaofferid,priority 
from offer_incentive_final_allocations_union_all_dy_4 where mail_opt_in_ind<>0;  --DM1 results here



--up to 10 vendor offers
--up to 1 product offer
--rest are offer bank offers

--stack vendor offers with product offers
drop table vendor_prod_ct;
create temp table vendor_prod_ct as
select * from catalina_offer_pool where type in ('vendor','product');

--apply super rules for the first time
drop table vendor_prod_super1;
create temp table vendor_prod_super1 as
select * from (select *, row_number()over(partition by acct_id, precima_ofb_id order by priority) as ofb_rank from vendor_prod_ct)a
where ofb_rank = 1;
--Super Group rule2: 20% within class level
drop table vendor_prod_super2;
create temp table vendor_prod_super2 as
select * from (select *, row_number()over(partition by acct_id, offer_bank_group_code order by priority) as class_rank from vendor_prod_super1)a
where class_rank <= 6;
--Super Group rule3: 33% within class level
drop table vendor_prod_super3;
create temp table vendor_prod_super3 as
select * from (select *, row_number()over(partition by acct_id, offer_bank_supergroup_code order by priority) as super_rank from vendor_prod_super2)a
where super_rank <= 8;

--pick the highest ranking 10 vendor offers and the top ranking product offer
drop table vendor_prod_offer;
create table vendor_prod_offer as
select * from
(select *, row_number()over(partition by acct_id, type order by v10 desc) as type_rank,
case when  type='vendor' and type_rank <=10 then 1 when type='product' and type_rank <=1 then 1 else 0 end as flag
from vendor_prod_super3)a
where flag =1;

alter table vendor_prod_offer drop column ofb_rank,class_rank,super_rank,type_rank,flag restrict;


--stack with offer bank
drop table vendor_prod_ofb_ct;
create temp table vendor_prod_ofb_ct as
select * from (select * from catalina_offer_pool where type='ofb')a union select * from vendor_prod_offer;


--apply super rules
drop table offer_bank_supergroup_code1;
create temp table offer_bank_supergroup_code1 as
select * from (select *, row_number()over(partition by acct_id, precima_ofb_id order by priority) as ofb_rank from vendor_prod_ofb_ct)a
where ofb_rank = 1;
--Super Group rule2: 20% within class level
drop table offer_bank_supergroup_code2;
create temp table offer_bank_supergroup_code2 as
select * from (select *, row_number()over(partition by acct_id, offer_bank_group_code order by priority) as class_rank from offer_bank_supergroup_code1)a
where class_rank <= 0.2 * 32;
--Super Group rule3: 33% within class level
drop table offer_bank_supergroup_code3;
create temp table offer_bank_supergroup_code3 as
select * from (select *, row_number()over(partition by acct_id, offer_bank_supergroup_code order by priority) as super_rank from offer_bank_supergroup_code2)a
where super_rank <= 0.33 * 32;


--pick the final 32 offers

drop table  offer_assignment_final_ct;
create table offer_assignment_final_ct as
select *,'Catalina' as channel from (select *, row_number()over(partition by acct_id order by priority, v10 desc) as final_rank from offer_bank_supergroup_code3)a
where final_rank <= 32;

alter table offer_assignment_final_ct drop column ofb_rank,class_rank,super_rank,final_rank restrict;

--customers with fewer than 32 offers are not qualified
delete from offer_assignment_final_ct where acct_id in (select acct_id from (select acct_id, count(*) as offer_cnt from offer_assignment_final_ct group by 1 having offer_cnt < 32)a);


--select count(distinct acct_id) from offer_assignment_final_ct --4,446,344

/*************************************************/
/*******Step ?: join with member table************/
/*********transfer from CARD to CUSTOMER**********/
/*************************************************/

drop table temp8_assignment:user;
create temp table temp8_assignment:user as
select b.cust_acct_key,a.* from
offer_assignment_final_ct a
left join
universe_custs_msn_:user b -- This table is created in part 1
on a.acct_id=b.card_nbr_hash
where cust_acct_key not in 
(select distinct cust_acct_key
	from
		(select cust_acct_key,count(distinct card_nbr_hash) as custs from universe_custs_msn_:user group by 1) a 
	where custs>1) 
--and b.more_card_status = 'Registered' -- this is taken care of in the universe_cust_msn creation
;

drop table offer_opt_in_status;
create temp table offer_opt_in_status as
select b.account_number, a.*, b.email_opt_in_ind, b.mail_opt_in_ind,b.phone_opt_in_ind,b.sms_opt_in_ind from  --will report on ACCOUNT_NUMBER
temp8_assignment:user a
left join
msn_member b
on a.cust_acct_key=b.cust_acct_key
--where deceased_flag=0 -- This is taken care of in the previous step
;


/*************************************************/
/*******Step 6: Decide on incentive level*********/
/*************************************************/


--vendor
drop table vendor_incentive;
create temp table vendor_incentive as
select a.*,b.maxincentive as incentive_tmp
from
offer_opt_in_status a
left join
msn_rd_vendor_offer_table_20160802dyang  b
on a.precimavendorid=b.precimavendorid 
and a.precimaofferid=b.precimaofferid
where a.type='vendor' and b.offer_class='Vendor';

--product
drop table product_incentive;
create temp table product_incentive as
select a.*, b.maxincentive as incentive_tmp from
offer_opt_in_status a
left join
:final_vendor_offer_table:user b
on a.precimavendorid=b.precimavendorid 
and a.precimaofferid=b.precimaofferid
where type='product' and b.offer_class='Product';

--offer bank level incentive
drop table ofb_incentive;
create temp table ofb_incentive as
select a.*, b.disc_ofb_sales as incentive_tmp from
offer_opt_in_status a
left join
avg_txn_sales_ofb_id_all b
on a.cust_acct_key=b.cust_acct_key and a.precima_ofb_id=b.precima_ofb_id
where type='ofb';

--union
create temp table temp_all_incentive_union as
(select * from vendor_incentive 
union
select * from product_incentive 
union
select * from ofb_incentive);

drop table temp_all_incentive_union_1;
create table temp_all_incentive_union_1 as
select a.*,
case when priority=3 then b.disc_cap_n_collared else 0 end as disc_cap_n_collared,
case when priority=3 then b.incentive_min else incentive_tmp end as incentive_min,
case when priority=3 then b.incentive_max else incentive_tmp end as incentive_max,
case when priority=3 then b.incentive_incrementals else 0 end as incentive_incrementals
from 
(select a.*,b.avg_ofb_txns
from
temp_all_incentive_union a
left join
(select cust_acct_key,precima_ofb_id,avg_ofb_txns from avg_txn_sales_ofb_id_all group by 1,2,3) b
on a.cust_acct_key=b.cust_acct_key
and a.precima_ofb_id=b.precima_ofb_id
) a
left join
(select precima_ofb_id,disc_cap_n_collared,incentive_min,incentive_max,incentive_incrementals from :final_offer_bank_table:user group by 1,2,3,4,5) b
on a.precima_ofb_id=b.precima_ofb_id;

drop table offer_incentive_final_allocations_catalina;
create table offer_incentive_final_allocations_catalina as
select *,
case when a.precima_ofb_id in ('MOR-26','MOR-89','MOR-62','MOR-67','MOR-11','MOR-86') and ((incentive_tmp*1000)<=incentive_min) then 1.0*incentive_min/1000 
when a.precima_ofb_id in ('MOR-26','MOR-89','MOR-62','MOR-67','MOR-11','MOR-86') and ((incentive_tmp*1000)>=incentive_max) then 1.0*incentive_max/1000
when a.precima_ofb_id not in ('MOR-26','MOR-89','MOR-62','MOR-67','MOR-11','MOR-86') and (incentive_tmp<=incentive_min) then incentive_min 
when a.precima_ofb_id not in ('MOR-26','MOR-89','MOR-62','MOR-67','MOR-11','MOR-86') and (incentive_tmp>=incentive_max) then incentive_max
else incentive_tmp end as incentive_final
from temp_all_incentive_union_1 a;



Grant list, select on offer_incentive_final_allocations_catalina to pprcmmrn01_usr_read ;


/*********************************************************************************/
/*************************ADJUST INCENTIVE TYPE***********************************/
/*********************************************************************************/


/*first column: adjusted incentive rate*/

drop table offer_incentive_final_allocations_catalina_1;
create temp table offer_incentive_final_allocations_catalina_1 as
select a.*, b.incentive_1,b.incentive_2,b.incentive_3,b.pe,b.avg_amt
from offer_incentive_final_allocations_catalina a
left join (select acct_id, ofb_id, pe, avg_amt, incentive_1,incentive_2,incentive_3 from dy_garcia_acct_ofb_inc_optimal group by 1,2,3,4,5,6,7) b
on a.acct_id=b.acct_id and a.precima_ofb_id=b.ofb_id;

drop table offer_incentive_final_allocations_catalina_2;
create temp table offer_incentive_final_allocations_catalina_2 as
select *, 
case when incentive_tmp <= incentive_min then incentive_min when incentive_tmp >= incentive_max then incentive_max when incentive_tmp is null then incentive_min else incentive_tmp end as inc_adjusted,
case when round(inc_adjusted,2) != round(incentive_min,2) and round(inc_adjusted,2) != round(incentive_1,2) and round(inc_adjusted,2) != round(incentive_2,2) and round(inc_adjusted,2) != round(incentive_3,2) and round(inc_adjusted,2) != round(incentive_max,2) then (inc_adjusted - 0.05) end as inc_lower,
inc_lower + 0.1 as inc_upper,
case when pe between -6 and -1 then inc_upper when pe between -1 and -0.5 then inc_lower end as inc_bound_adjusted,
case when inc_bound_adjusted is null then inc_adjusted else inc_bound_adjusted end as inc_bound_temp
from  offer_incentive_final_allocations_catalina_1; 


drop table offer_incentive_final_allocations_catalina_3;
create table offer_incentive_final_allocations_catalina_3 as
select *, 
case when precima_ofb_id in  ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67') then inc_bound_temp/1000 else inc_bound_temp end as inc_bound_final 
from
(
select account_number,cust_acct_key,cohort,acct_id,item1,v10,rank,purch_flag,precima_ofb_id,offer_bank_group_code,offer_bank_supergroup_code,type,precimavendorid,precimaofferid,priority,channel,email_opt_in_ind,mail_opt_in_ind,phone_opt_in_ind,sms_opt_in_ind,incentive_tmp,avg_ofb_txns,disc_cap_n_collared,incentive_min,incentive_max,incentive_incrementals,incentive_final,inc_bound_temp
from offer_incentive_final_allocations_catalina_2 where type='ofb'
union
select *, incentive_final as inc_bound_temp from offer_incentive_final_allocations_catalina  where type='vendor'
union
select *, incentive_final as inc_bound_temp from offer_incentive_final_allocations_catalina where type='product'
)a;


/*********************************************************************************/
/*************************BUDGET ALLOCATION STEP**********************************/
/*********************************************************************************/

--QA: inc_bound_final null / # of offers per customers / super group rules
--include only non-mail-opt-in customers


--select count(distinct acct_id) from offer_incentive_final_allocations_catalina_3 where mail_opt_in_ind =0
--1,022,766

drop table offer_incentive_final_allocations_catalina_non_mail;
create table offer_incentive_final_allocations_catalina_non_mail as
select * from offer_incentive_final_allocations_catalina_3 where mail_opt_in_ind=0;


--vendor customer offer assgmt
drop table Vendor_custs_part1;  --164,727
create temp table Vendor_custs_part1 as
select * from
offer_incentive_final_allocations_catalina_non_mail 
where cust_acct_key in (select cust_acct_key from offer_incentive_final_allocations_catalina_non_mail where type='vendor');


-- sampling universe: non vendor customers, without null potential spend customers

drop table sampling_universe_tmp;  --858,039
create temp table sampling_universe_tmp as
select distinct cust_acct_key, acct_id from offer_incentive_final_allocations_catalina_non_mail
except
select distinct cust_acct_key, acct_id from Vendor_custs_part1;

drop table sampling_universe; --842092
create table sampling_universe as
select a.*,b.potential_spend_segment from
sampling_universe_tmp a
left join
:customer_table b
on a.cust_acct_key=b.cust_acct_key
where potential_spend_segment is not null;

--using sas to do stratified sampling

--select count(*) from dy_sample_result_catalina; 84,212 

--exclude sample customers
drop table not_sampled_universe;
create table not_sampled_universe as
select cust_acct_key,potential_spend_segment from sampling_universe
except
select cust_acct_key,potential_spend_segment from dy_sample_result_catalina;

--cust list
drop table priority_custs_list_catalina;
create table priority_custs_list_catalina as  --1006819
select 1 as priority_custs_tmp, 'vendor' as cust_category, cust_acct_key from (select distinct cust_acct_key from Vendor_custs_part1)a
union
select 2 as priority_custs_tmp, 'sample' as cust_category, cust_acct_key from dy_sample_result_catalina
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


create temp table offer_incentive_final_allocations_catalina_non_mail_2 as
select *, case when priority_custs_tmp is null then 12 else priority_custs_tmp end as priority_custs 
from
(select b.priority_custs_tmp, b.cust_category, a.* from
offer_incentive_final_allocations_catalina_non_mail a
left join
priority_custs_list_catalina b
on a.cust_acct_key=b.cust_acct_key)a;


select priority_custs,cust_category,type,
case when type='vendor' then 0 when type='product' then 0.7 when type='ofb' then 0.7 else 1 end as resp_rate,
count(distinct account_number) as custs,
sum(offers) as offers,
sum(rebate) as total_rebate,
total_rebate*resp_rate as cost_redemption
from
(select priority_custs,cust_category,account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from offer_incentive_final_allocations_catalina_non_mail_2 
where mail_opt_in_ind <> 0
group by 1,2,3,4)
group by 1,2,3,4


/**/