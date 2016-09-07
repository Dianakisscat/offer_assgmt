


/*************************************************/
/*****TOTAL OFFER POOL FOR ALL CUSTOMERS**********/
/*************************************************/

drop table all_custs_offer_pool;
create table all_custs_offer_pool as
select 1 as cohort,* from all_offers_union_1dyang 
union all
select 2 as cohort,* from all_offers_union_2dyang 
union all
select 3 as cohort,* from all_offers_union_3dyang 
union all
select 4 as cohort,* from all_offers_union_4dyang 
union all
select 5 as cohort,* from all_offers_union_5dyang 
union all
select 6 as cohort,* from all_offers_union_6dyang 
union all
select 7 as cohort,* from all_offers_union_7dyang 
union all
select 8 as cohort,* from all_offers_union_8dyang 
union all
select 9 as cohort,* from all_offers_union_9dyang 
union all
select 10 as cohort,* from all_offers_union_10dyang 
union all
select 11 as cohort,* from all_offers_union_11dyang 
union all
select 12 as cohort,* from all_offers_union_12dyang 
union all
select 13 as cohort,* from all_offers_union_13dyang 
union all
select 14 as cohort,* from all_offers_union_14dyang 
union all
select 15 as cohort,* from all_offers_union_15dyang 
union all
select 16 as cohort,* from all_offers_union_16dyang 
union all
select 17 as cohort,* from all_offers_union_17dyang 
union all
select 18 as cohort,* from all_offers_union_18dyang
union all
select 19 as cohort,* from all_offers_union_19dyang 
union all
select 20 as cohort,* from all_offers_union_20dyang
union all
select 21 as cohort,* from all_offers_union_21dyang
union all
select 22 as cohort,* from all_offers_union_22dyang
union all
select 23 as cohort,* from all_offers_union_23dyang
union all
select 24 as cohort,* from all_offers_union_24dyang
union all
select 25 as cohort,* from all_offers_union_25dyang
union all
select 26 as cohort,* from all_offers_union_26dyang
union all
select 27 as cohort,* from all_offers_union_27dyang
union all
select 28 as cohort,* from all_offers_union_28dyang
union all
select 29 as cohort,* from all_offers_union_29dyang
union all
select 30 as cohort,* from all_offers_union_30dyang;


Grant list, select on all_custs_offer_pool to pprcmmrn01_usr_read ;


/***********************************************************/
/**************SAME AUDIENCE IN TEST CONTROL****************/
/***********************************************************/



create table all_custs_offer_pool_selected as
	select * from all_custs_offer_pool where acct_id in (select acct_id from tc_test_result_catalina);


/*************************************************/
/**************DELETE WRONG OFFERS****************/
/*************************************************/

------------Check offer pool
drop table check_acct_ofb_purch_final;  -- customers purchased from these suspicious offer banks
create temp table check_acct_ofb_purch_final as
select acct_id, precima_ofb_id 
from all_custs_offer_pool_selected a
group by 1,2;

------------check if they purchased any of the correct prod_hier_keys
drop table acct_ofb_cust_tlog;
create temp table acct_ofb_cust_tlog as 
select  a.acct_id, a.tran_id,a.tran_dt
from :cust_tlog_table a, check_acct_ofb_purch_final b           --pull cust tlog hist, get tran_id
where a.tran_dt between :strt_dt and :end_dt
and a.acct_id=b.acct_id group by 1,2,3;

drop table acct_ofb_tlog;
create temp table acct_ofb_tlog as 
select a.*, b.prod_id, sum(b.sale_amt) as sale_amt        --pull tlog hist, get prod_id
from acct_ofb_cust_tlog a, :tlog_table b
where a.tran_dt between :strt_dt and :end_dt
and a.tran_id=b.tran_id group by 1,2,3,4;


drop table acct_ofb_prod_hier;                            --get prod_hier_key
create temp table acct_ofb_prod_hier as    
select a.*, b.prod_hier_id_lvl3||'-'||b.prod_hier_id_lvl2||'-'||b.prod_hier_id_lvl1 as prod_hierarchy_key,1 as purch_flag 
from acct_ofb_tlog a,:product_table b 
where a.prod_id=b.prod_id;


drop table acct_ofb_check_purch;                          
create temp table acct_ofb_check_purch as
select a.*, b.precima_ofb_id
from acct_ofb_prod_hier a, MSN_campaign_offer_Bank_hist b
where a.prod_hierarchy_key=b.prod_hierarchy_key 
and promoted_flag != 'N'
;   --filter, make sure customers are assigned offer for a certain category



--Delete those assigned wrong offers 
delete from all_custs_offer_pool_selected where (acct_id,precima_ofb_id) in 
	(select acct_id,precima_ofb_id from check_acct_ofb_purch_final except select acct_id,precima_ofb_id from acct_ofb_check_purch);

delete from all_custs_offer_pool_selected where precima_ofb_id not in (select precima_ofb_id  from :offer_bank_table);

Grant list, select on all_custs_offer_pool_selected to pprcmmrn01_usr_read;


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


Grant list, select on avg_txn_sales_ofb_id_all to pprcmmrn01_usr_read;

/*************************************************/
/*************Prepare PE Lookup Table*************/
/*************************************************/

--when pe between (-6,-1), choose the max incentive that is SMALLER than avg_amt;   
--when pe between (-1, -0.5) OR incentive_min > avg_amt, choose incentive_min straightforwardly
/*
create temp table garcia_cust_ofb_incentive_lift_catalina_tmp as
	select *, case when avg_amt is null then 0 else avg_amt end as avg_amt_v2 from garcia_cust_ofb_incentive_lift_catalina;

create temp table vertical_pe as
select * from garcia_cust_ofb_incentive_lift_catalina_tmp where (pe between -6 and -1);

create temp table vertical_pe_union as
select distinct cust_acct_key,ofb_id,avg_amt,incentive_min as tmp from vertical_pe union all
select distinct cust_acct_key,ofb_id,avg_amt,incentive_1 as tmp from vertical_pe union all
select distinct cust_acct_key,ofb_id,avg_amt,incentive_2 as tmp from vertical_pe union all
select distinct cust_acct_key,ofb_id,avg_amt,incentive_3 as tmp from vertical_pe union all
select distinct cust_acct_key,ofb_id,avg_amt,incentive_max as tmp from vertical_pe;

drop table vertical_pe_max_lift;
create temp table vertical_pe_max_lift as
select distinct cust_acct_key, ofb_id as precima_ofb_id, max(tmp) as inc_optimal
from vertical_pe_union where tmp <= avg_amt
group by 1,2;

drop table dy_garcia_cust_ofb_inc_catalina;
create table dy_garcia_cust_ofb_inc_catalina as
(select *, incentive_min as inc_optimal 
 from garcia_cust_ofb_incentive_lift_catalina_tmp 
 where (pe<-0.5 and pe>-1)  or (incentive_min > avg_amt) or avg_amt is null)     --pe between (-1, -0.5) OR incentive_min > avg_amt 
union 
(select a.*, b.inc_optimal from
vertical_pe a, vertical_pe_max_lift b															  --pe between (-6,-1)
where a.cust_acct_key=b.cust_acct_key
and a.ofb_id=b.precima_ofb_id);

Grant list, select on dy_garcia_cust_ofb_inc_catalina to pprcmmrn01_usr_read ;
*/

-------------------------------------------------------------------------------
/*************************************************/
/*****CATALINA OFFER ASSIGNMENT PROCESS***********/
/*************************************************/

--CATALINA Offer pool: excluding DM1 from the original offer pool
drop table catalina_offer_pool;
create temp table catalina_offer_pool as
select * from all_custs_offer_pool_selected where acct_id not in (select acct_id from tc_test_result_dm1);--priority_custs_list_dm1_final);



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
select * from (select *, row_number()over(partition by acct_id, precima_ofb_id order by priority,v10 desc) as ofb_rank from vendor_prod_ct)a
where ofb_rank = 1;
--Super Group rule2: 20% within class level
drop table vendor_prod_super2;
create temp table vendor_prod_super2 as
select * from (select *, row_number()over(partition by acct_id, offer_bank_group_code order by priority,v10 desc) as class_rank from vendor_prod_super1)a
where class_rank <= 6;
--Super Group rule3: 33% within class level
drop table vendor_prod_super3;
create temp table vendor_prod_super3 as
select * from (select *, row_number()over(partition by acct_id, offer_bank_supergroup_code order by priority,v10 desc) as super_rank from vendor_prod_super2)a
where super_rank <= 10;

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
select * from (select *, row_number()over(partition by acct_id, precima_ofb_id order by priority,v10 desc) as ofb_rank from vendor_prod_ofb_ct)a
where ofb_rank = 1;
--Super Group rule2: 20% within class level
drop table offer_bank_supergroup_code2;
create temp table offer_bank_supergroup_code2 as
select * from (select *, row_number()over(partition by acct_id, offer_bank_group_code order by priority,v10 desc) as class_rank from offer_bank_supergroup_code1)a
where class_rank <= 6;
--Super Group rule3: 33% within class level
drop table offer_bank_supergroup_code3;
create temp table offer_bank_supergroup_code3 as
select * from (select *, row_number()over(partition by acct_id, offer_bank_supergroup_code order by priority,v10 desc) as super_rank from offer_bank_supergroup_code2)a
where super_rank <= 10;


--pick the final 32 offers

drop table  offer_assignment_final_ct;
create table offer_assignment_final_ct as
select *,'Catalina' as channel from (select *, row_number()over(partition by acct_id order by priority, v10 desc) as final_rank from offer_bank_supergroup_code3)a
where final_rank <= 32;

alter table offer_assignment_final_ct drop column ofb_rank,class_rank,super_rank,final_rank restrict;

--customers with fewer than 32 offers are not qualified
delete from offer_assignment_final_ct where acct_id in 
	(select acct_id from (select acct_id, count(*) as offer_cnt from offer_assignment_final_ct group by 1 having offer_cnt < 32)a);


--select count(distinct acct_id) from offer_assignment_final_ct --4,446,344

/*************************************************/
/***************join with member table************/
/*********transfer from CARD to CUSTOMER**********/
/*************************************************/

drop table temp8_assignment:user;
create temp table temp8_assignment:user as
select b.cust_acct_key,a.* 
from
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
/*************Decide on incentive level***********/
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
msn_rd_vendor_offer_table_20160802dyang b
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
drop table temp_all_incentive_union;
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

--change point based incentive_max and min to pound based
drop table offer_incentive_final_allocations_catalina_0;
create temp table offer_incentive_final_allocations_catalina_0 as
select *, case when precima_ofb_id in  ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67') then incentive_max/1000 else incentive_max end as inc_max_tmp,
case when precima_ofb_id in  ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67') then incentive_min/1000 else incentive_min end as inc_min_tmp,
case when precima_ofb_id in  ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67') then incentive_incrementals/1000 else incentive_incrementals end as incremental_tmp
from offer_incentive_final_allocations_catalina;

/*first column: adjusted incentive rate*/

/*
create table dy_garcia_cust_ofb_incentive_lift_catalina_v3 as
select * from garcia_cust_ofb_incentive_lift_catalina_v2
union
select * from garcia_cust_ofb_incentive_lift_new where cust_acct_key in (select cust_acct_key from priority_custs_list_dm1_final where priority_custs in (10,11,12));

create table dy_garcia_cust_ofb_incentive_lift_catalina_v4 as
select * from (select *, row_number()over(partition by cust_acct_key,ofb_id order by pe) as tmp from dy_garcia_cust_ofb_incentive_lift_catalina_v3)a where tmp=1
*/


drop table offer_incentive_final_allocations_catalina_1;
create temp table offer_incentive_final_allocations_catalina_1 as
select a.*, round(b.incentive_1,2) as incentive_1,round(b.incentive_2,2) as incentive_2, round(b.incentive_3,2) as incentive_3,
case when b.pe is not null then pe else -0.5 end as pe, b.avg_amt
from offer_incentive_final_allocations_catalina_0 a
left join (select cust_acct_key, ofb_id, pe, avg_amt, incentive_1,incentive_2,incentive_3 from dy_garcia_cust_ofb_incentive_lift_catalina_v4 group by 1,2,3,4,5,6,7) b
on a.cust_acct_key=b.cust_acct_key and a.precima_ofb_id=b.ofb_id;

drop table offer_incentive_final_allocations_catalina_2;
create table offer_incentive_final_allocations_catalina_2 as
select *,
case when incentive_tmp <= inc_min_tmp then inc_min_tmp when incentive_tmp >= inc_max_tmp then inc_max_tmp when incentive_tmp is null then inc_min_tmp else incentive_tmp end as inc_adjusted,
case when
(inc_adjusted != inc_min_tmp and inc_adjusted!= incentive_1 and inc_adjusted!= incentive_2  and inc_adjusted != inc_max_tmp )
-- and inc_adjusted != incentive_3 and inc_adjusted!= incentive_2  ) 
then (inc_adjusted - 0.05) end as inc_lower, inc_lower + 0.1 as inc_upper,
case when pe between -6 and -1 then inc_upper when pe between -1 and -0.5 then inc_lower end as inc_bound_adjusted,
case when inc_bound_adjusted is null then inc_adjusted else inc_bound_adjusted end as inc_bound_final
from  offer_incentive_final_allocations_catalina_1 where incentive_3 is null
union all
select *,
case when incentive_tmp <= inc_min_tmp then inc_min_tmp when incentive_tmp >= inc_max_tmp then inc_max_tmp when incentive_tmp is null then inc_min_tmp else incentive_tmp end as inc_adjusted,
case when
(inc_adjusted != inc_min_tmp and inc_adjusted!= incentive_1 and inc_adjusted!= incentive_2 and inc_adjusted != incentive_3 and inc_adjusted != inc_max_tmp )
--  and inc_adjusted!= incentive_2  ) 
then (inc_adjusted - 0.05) end as inc_lower, inc_lower + 0.1 as inc_upper,
case when pe between -6 and -1 then inc_upper when pe between -1 and -0.5 then inc_lower end as inc_bound_adjusted,
case when inc_bound_adjusted is null then inc_adjusted else inc_bound_adjusted end as inc_bound_final
from  offer_incentive_final_allocations_catalina_1 where incentive_3 is not null; 

Grant list, select on offer_incentive_final_allocations_catalina_2 to pprcmmrn01_usr_read;

/*second column: purely optimal rate based on PE*/
/*
drop table offer_incentive_final_allocations_catalina_3;
create table offer_incentive_final_allocations_catalina_3 as
select *,
case 
when inc_optimal is null then inc_min_tmp 
else inc_optimal end as inc_optimal_final
from (select a.*, b.inc_optimal from 
offer_incentive_final_allocations_catalina_2 a
left join
dy_garcia_cust_ofb_inc_optimal b
on a.cust_acct_key=b.cust_acct_key and a.precima_ofb_id=b.ofb_id)a
;

Grant list, select on offer_incentive_final_allocations_catalina_3 to pprcmmrn01_usr_read;

*/


/*********************************************************************************/
/*************************BUDGET ALLOCATION STEP**********************************/
/*********************************************************************************/


--vendor customer offer assgmt
drop table Vendor_custs_part1;  --
create temp table Vendor_custs_part1 as
select * from
offer_incentive_final_allocations_catalina_2 
where cust_acct_key in (select cust_acct_key from offer_incentive_final_allocations_catalina_2 where type='vendor');




drop table offer_incentive_final_allocations_catalina_non_mail;
create temp table offer_incentive_final_allocations_catalina_non_mail as
select * from offer_incentive_final_allocations_catalina_2 
where 
mail_opt_in_ind=0;


-- sampling universe: non vendor customers, without null potential spend customers
drop table sampling_universe_tmp;  --871,465
create temp table sampling_universe_tmp as
select distinct cust_acct_key, acct_id from offer_incentive_final_allocations_catalina_non_mail
except
select distinct cust_acct_key, acct_id from Vendor_custs_part1;

drop table sampling_universe; --854331
create table sampling_universe as
select a.*,b.potential_spend_segment from
sampling_universe_tmp a
left join
:customer_table b
on a.cust_acct_key=b.cust_acct_key
where potential_spend_segment is not null;

drop table sampling_universe_final;
create table sampling_universe_final as
select
cast(cust_acct_key as integer) as cust_acct_key,potential_spend_segment from sampling_universe; 

--using sas to do stratified sampling

--select count(*) from dy_sample_result_catalina; 85,437

--exclude sample customers
drop table not_sampled_universe;
create table not_sampled_universe as
select cust_acct_key,potential_spend_segment from sampling_universe
except
select cust_acct_key,potential_spend_segment from dy_sample_result_catalina;


--include only non-mail-opt-in customers
--select count(distinct acct_id) from offer_incentive_final_allocations_catalina_3 where mail_opt_in_ind =0
--1,022,766

--cust list

drop table priority_custs_list_catalina;
create table priority_custs_list_catalina as  
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


drop table offer_incentive_final_allocations_catalina_non_mail_2;
create temp table offer_incentive_final_allocations_catalina_non_mail_2 as
select *, case when priority_custs_tmp is null then 12 else priority_custs_tmp end as priority_custs 
from
(select b.priority_custs_tmp, b.cust_category, a.* from
offer_incentive_final_allocations_catalina_non_mail a
left join
priority_custs_list_catalina b
on a.cust_acct_key=b.cust_acct_key)a;


select priority_custs, count(distinct cust_acct_key) as cnt from offer_incentive_final_allocations_catalina_non_mail_2 group by 1;

--------------Back up CATALINA
drop table offer_incentive_final_allocations_catalina_backup;
create temp table offer_incentive_final_allocations_catalina_backup as
select a.*,b.priority_custs,b.cust_category 
from offer_incentive_final_allocations_catalina_2 a, priority_custs_list_dm1_final b
where a.cust_acct_key=b.cust_acct_key
and a.mail_opt_in_ind=1
and b.priority_custs in (10,11,12);


------------------------------------------------------
------------------------------------------------------
--------------Custs deleted from DM1 added to CATALINA
------------------------------------------------------
------------------------------------------------------
------------------------------------------------------

/*
drop table offer_incentive_final_allocations_catalina_added_dm1;
create temp table offer_incentive_final_allocations_catalina_added_dm1 as
select a.* from 
offer_incentive_final_allocations_catalina_3 a,
delete_custs b,
delete_custs_v2 c
where a.cust_acct_key=b.cust_acct_key or a.cust_acct_key=c.cust_acct_key
and mail_opt_in_ind=1;
*/



--------------All CATALINA OFFER INCLUDING LOADED AND BACKUP
drop table offer_incentive_final_allocations_catalina_all;
create table offer_incentive_final_allocations_catalina_all  as
select 
mail_opt_in_ind,priority_custs,cust_category,account_number,cust_acct_key,acct_id,cohort,item1,rank,precima_ofb_id,offer_bank_group_code,offer_bank_supergroup_code,type,precimavendorid,precimaofferid,priority,incentive_tmp,incentive_min,incentive_max,incentive_incrementals,incentive_final,inc_max_tmp,inc_min_tmp,incentive_1,incentive_2,incentive_3,pe,inc_adjusted,inc_lower,inc_upper,inc_bound_adjusted,inc_bound_final
from  offer_incentive_final_allocations_catalina_non_mail_2
union all
select 
mail_opt_in_ind,priority_custs,cust_category,account_number,cust_acct_key,acct_id,cohort,item1,rank,precima_ofb_id,offer_bank_group_code,offer_bank_supergroup_code,type,precimavendorid,precimaofferid,priority,incentive_tmp,incentive_min,incentive_max,incentive_incrementals,incentive_final,inc_max_tmp,inc_min_tmp,incentive_1,incentive_2,incentive_3,pe,inc_adjusted,inc_lower,inc_upper,inc_bound_adjusted,inc_bound_final
from  offer_incentive_final_allocations_catalina_backup
;
/*union all
select 
mail_opt_in_ind,13 as priority_custs,'added from DM' as cust_category,account_number,cust_acct_key,acct_id,cohort,item1,rank,precima_ofb_id,offer_bank_group_code,offer_bank_supergroup_code,type,precimavendorid,precimaofferid,priority,incentive_tmp,incentive_min,incentive_max,incentive_incrementals,incentive_final,inc_max_tmp,inc_min_tmp,incentive_1,incentive_2,incentive_3,pe,inc_adjusted,inc_lower,inc_upper,inc_bound_adjusted,inc_bound_final,inc_optimal,inc_optimal_final
from offer_incentive_final_allocations_catalina_added_dm1;
*/

--QA
select distinct cust_acct_key from offer_incentive_final_allocations_catalina_backup 
intersect 
select distinct cust_acct_key from offer_incentive_final_allocations_catalina_non_mail_2;


select distinct cust_acct_key from offer_incentive_final_allocations_catalina_all 
intersect 
select distinct cust_acct_key from final_offer_assgmt_window_dm1;

--delete duplicated customers from DM1, due to MSN_MEMBER refreshment
delete from offer_incentive_final_allocations_catalina_all where cust_acct_key in (select cust_acct_key from final_offer_assgmt_window_dm1);


---------------ALL CATALINA CUSTOMER LISTS (including loaded and backup)

drop table priority_custs_list_catalina_final;   
create table priority_custs_list_catalina_final as
select mail_opt_in_ind,priority_custs,cust_category,cust_acct_key from offer_incentive_final_allocations_catalina_all group by 1,2,3,4;


Grant list, select on priority_custs_list_catalina_final to pprcmmrn01_usr_read;


--Point based offers changed
--Add back up priority number
drop table final_offer_assgmt_table_catalina;
create table final_offer_assgmt_table_catalina as
select *, case 
when mail_opt_in_ind = 0 and priority_custs =1 then 7 
when mail_opt_in_ind = 0 and priority_custs =5 then 2
when mail_opt_in_ind = 0 and priority_custs =6 then 4
when mail_opt_in_ind = 0 and priority_custs =7 then 5
when mail_opt_in_ind = 0 and priority_custs =8 then 3
when mail_opt_in_ind = 0 and priority_custs =9 then 13
when mail_opt_in_ind = 0 and priority_custs =10 then 15
when mail_opt_in_ind = 0 and priority_custs =11 then 17
when mail_opt_in_ind = 0 and priority_custs =12 then 19 
when mail_opt_in_ind != 0 and priority_custs =10 then 14 
when mail_opt_in_ind != 0 and priority_custs =11 then 16 
when mail_opt_in_ind != 0 and priority_custs =12 then 18 
end as backup_priority,
case 
when incentive_type='Points' then round((inc_bound_final * 1000),0) else round(inc_bound_final,2) end as incentive_print
from
(select a.*,c.incentive_type from 
offer_incentive_final_allocations_catalina_all a,
msn_campaign_offer_bank_ty_incentive c
where 
a.precima_ofb_id=c.precima_ofb_id)a;


/***************************************************/
/******************Period Placement*****************/
/***************************************************/

drop table final_offer_assgmt_table_catalina_period;
create temp table final_offer_assgmt_table_catalina_period as
select *, case
	when offer_position_nbr between 1 and 8 then '2016-11-07'
	when offer_position_nbr between 9 and 16 then '2016-11-21'
	when offer_position_nbr between 17 and 24 then '2016-12-05'
	when offer_position_nbr between 25 and 32 then '2016-12-19' end as coupon_start_dt,
	case
	when offer_position_nbr between 1 and 8 then '2016-11-20'
	when offer_position_nbr between 9 and 16 then '2016-12-04'
	when offer_position_nbr between 17 and 24 then '2016-12-18'
	when offer_position_nbr between 25 and 32 then '2017-01-01' end as coupon_end_dt
from (select *, row_number()over(partition by acct_id order by priority_tmp, rank) as offer_position_nbr 
from (select *, case when priority=1 then 1 when priority=2 then 3 when priority=3 then 2 end as priority_tmp 
	  from final_offer_assgmt_table_catalina
)a)a;


/***************************************************/
/******************Shopping Frequency***************/
/***************************************************/


--32 offers in total, 8 week campagin period, 6 weeks redemption peiod (42 days)
--the number of coupons ranges from 1 to 4
	--1(32): if # of trips >=32
	--2(16): if # of trips in [16,32)
	--3(10.67): if # of trips in [11,16)
	--4(8): if # of trips < 11

drop table DM1_cust_shop_freq;
create temp table DM1_cust_shop_freq as
select a.acct_id, case when coupon_per_trip_tmp is null then 4 else coupon_per_trip_tmp end as coupon_per_trip
from
final_offer_assgmt_table_catalina_period a 
left join
(select *, case when six_week_trips >= 32 then 1 
	 when six_week_trips >= 16 and six_week_trips <= 32 then 2
	 when six_week_trips >= 11 and six_week_trips < 16 then 3
	 when six_week_trips < 11 then 4 end as coupon_per_trip_tmp
from
(select a.acct_id, count(distinct tran_id) as six_week_trips 
from :cust_tlog_table a, final_offer_assgmt_table_catalina_period b
where tran_dt between '2016-05-02' and '2016-06-12'
and a.acct_id=b.acct_id
group by 1)a) b
on a.acct_id=b.acct_id
group by 1,2;


drop table final_offer_assgmt_freq_catalina;
create table final_offer_assgmt_freq_catalina as
	select a.*, coupon_per_trip from 
	final_offer_assgmt_table_catalina_period a,
	DM1_cust_shop_freq b
	where a.acct_id=b.acct_id 
	and a.cust_acct_key in (select cust_acct_key from	mrsn_sbo_assignment_2016Xmas_50_neox);

delete from final_offer_assgmt_freq_catalina
where cust_acct_key in (select cust_acct_key from final_offer_assgmt_freq_catalina 
	where incentive_incrementals!=0 and mod((incentive_print-incentive_min),incentive_incrementals)!=0 );


Grant list, select on final_offer_assgmt_freq_catalina to pprcmmrn01_usr_read;




/***************************************************/
/******************Test Control Design**************/
/***************************************************/
drop table final_offer_assgmt_freq_catalina_treatment;
create table final_offer_assgmt_freq_catalina_treatment as
select a.*, b.tc_flag from
final_offer_assgmt_freq_catalina a left join tc_test_result_catalina b
on a.cust_acct_key=b.cust_acct_key
where tc_flag='T';

Grant list, select on final_offer_assgmt_freq_catalina_treatment to pprcmmrn01_usr_read;


--audience match
drop table tc_result_catalina_final;
create table tc_result_catalina_final as
select cust_acct_key, tc_flag from 
tc_test_result_catalina where cust_acct_key in (select cust_acct_key from final_offer_assgmt_freq_catalina);

Grant list, select on tc_result_catalina_final to pprcmmrn01_usr_read;
----------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------

---------------Rebate Summary


drop table ty_rebate_summary_catalina_final;
create table ty_rebate_summary_catalina_final as
select b.mail_opt_in_ind,b.priority_custs, b.cust_category, a.* from
(select a.cust_acct_key,a.total_rebate,b.vendor_rebate,c.product_rebate,d.ofb_rebate from
(select cust_acct_key, sum(inc_bound_final) as total_rebate from final_offer_assgmt_freq_catalina_treatment group by 1)a
left join
(select cust_acct_key, sum(inc_bound_final) as vendor_rebate from final_offer_assgmt_freq_catalina_treatment where type='vendor' group by 1)b
on a.cust_acct_key=b.cust_acct_key
left join
(select cust_acct_key, sum(inc_bound_final) as product_rebate from final_offer_assgmt_freq_catalina_treatment where type='product' group by 1)c
on a.cust_acct_key=c.cust_acct_key
left join
(select cust_acct_key, sum(inc_bound_final) as ofb_rebate from final_offer_assgmt_freq_catalina_treatment where type='ofb' group by 1)d
on a.cust_acct_key=d.cust_acct_key) a
left join
priority_custs_list_catalina_final b
on a.cust_acct_key=b.cust_acct_key;

Grant list, select on ty_rebate_summary_catalina_final to pprcmmrn01_usr_read;



/*************************************************/
/******************QA AND SUMMARIES***************/
/*************************************************/

--non promotition offer banks
create table non_promote_offer_bank_v1 as
select distinct precima_ofb_id from
(select  precima_ofb_id,count(distinct promoted_flag) as cnt  from :offer_bank_table  group by 1 having cnt =1)a
intersect 
select distinct precima_ofb_id from :offer_bank_table where promoted_flag='N';

select * from final_offer_assgmt_freq_catalina where precima_ofb_id in (select precima_ofb_id from non_promote_offer_bank_v1);



--offer bank dist
select a.*,b.offer_bank_name from
(select precima_ofb_id, count(*) as cnt from final_offer_assgmt_freq_catalina_treatment group by 1 )a
left join
:offer_bank_table b
on a.precima_ofb_id=b.precima_ofb_id group by 1,2,3

--Any customer from DM1?
--Should not be the case!

select cust_acct_key, count(*) as cnt from final_offer_assgmt_freq_catalina group by 1 having cnt !=32;

--cohort
select cohort,count(distinct cust_acct_key) from final_offer_assgmt_freq_catalina group by 1;
--account_number and cust_acct_key
select count(distinct cust_acct_key) from final_offer_assgmt_freq_catalina; --1945239
select count(distinct account_number) from final_offer_assgmt_freq_catalina; --1945239
select cust_acct_key,count(distinct account_number) as cnt from final_offer_assgmt_freq_catalina group by 1 having cnt > 1;
select account_number,count(distinct cust_acct_key) as cnt from final_offer_assgmt_freq_catalina group by 1 having cnt > 1;
--check null columns
select * from final_offer_assgmt_freq_catalina where inc_bound_final is null;
--check priority groups
select mail_opt_in_ind,priority_custs,cust_category,count(distinct cust_acct_key) as cnt from final_offer_assgmt_freq_catalina group by 1,2,3 order by 1,2;






/*
mail_opt_in_ind	priority_custs	cust_category	cnt
0	1	vendor	164150
0	2	sample	85414
0	3	HH	40401
0	4	HM	73566
0	5	HL	52826
0	6	MH	62613
0	7	MM	113229
0	8	ML	78032
0	9	LH	121034
0	10	LM	95914
0	11	LL	131144
1	10	LM	347714
1	11	LL	451890
1	13	added from DM	127312
*/
--check different offer types
select distinct type from final_offer_assgmt_freq_catalina;
select distinct type, priority from final_offer_assgmt_freq_catalina;
select distinct precimaofferid from final_offer_assgmt_freq_catalina where type='product';

--check incentive range and incentive type
select * from final_offer_assgmt_freq_catalina where incentive_print > incentive_max or incentive_print < incentive_min;
select * from final_offer_assgmt_freq_catalina where inc_bound_final > inc_max_tmp or incentive_print < inc_min_tmp;
select * from  final_offer_assgmt_freq_catalina where incentive_incrementals!=0 and mod((incentive_print-incentive_min),incentive_incrementals)!=0; 


select distinct incentive_type from final_offer_assgmt_freq_catalina where precima_ofb_id in ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67');

select distinct incentive_print from final_offer_assgmt_freq_catalina;

--cheeck super group rules
select acct_id,item1,count(*) as cnt from final_offer_assgmt_freq_catalina group by 1,2 having cnt >1;
select acct_id,precima_ofb_id,count(*) as cnt from final_offer_assgmt_freq_catalina group by 1,2 having cnt >1;
select acct_id,offer_bank_group_code, count(*) as cnt from final_offer_assgmt_freq_catalina group by 1,2 having cnt >6;
select acct_id,offer_bank_supergroup_code, count(*) as cnt from final_offer_assgmt_freq_catalina group by 1,2 having cnt >10;


--random check
select *  from final_offer_assgmt_freq_catalina where cust_acct_key =178904 and item1 in ('247','2');

