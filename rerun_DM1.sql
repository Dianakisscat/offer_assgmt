

/*************************************************/
/**************DELETE WRONG OFFERS****************/
/*************************************************/

drop table mail_list;
create temp table mail_list as 
	select distinct cust_acct_key from msn_member where mail_opt_in_ind=1;

drop table all_custs_offer_pool_dm1;
create table all_custs_offer_pool_dm1 as
	select b.cust_acct_key, a.* from 
	all_custs_offer_pool a left join msn_card_hist b
	on a.acct_id=b.card_nbr_hash
	where cust_acct_key in (select cust_acct_key from mail_list);

delete from all_custs_offer_pool_dm1 where cust_acct_key in 
	(select cust_acct_key, count(distinct acct_id) as cnt from all_custs_offer_pool_dm1 group by 1 having cnt >1);

drop table acct_ofb_cust_tlog;
create temp table acct_ofb_cust_tlog as 
select  a.acct_id, a.tran_id,a.tran_dt
from :cust_tlog_table a, all_custs_offer_pool_dm1 b           --pull cust tlog hist, get tran_id
--where a.tran_dt between :strt_dt and :end_dt
where a.acct_id=b.acct_id group by 1,2,3;

drop table acct_ofb_tlog;
create temp table acct_ofb_tlog as 
select a.*, b.prod_id, sum(b.sale_amt) as sale_amt        --pull tlog hist, get prod_id
from acct_ofb_cust_tlog a, :tlog_table b
--where a.tran_dt between :strt_dt and :end_dt
where a.tran_id=b.tran_id group by 1,2,3,4;


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
drop table all_custs_offer_pool_dm1_revised;
create table all_custs_offer_pool_dm1_revised as
select * from all_custs_offer_pool_dm1 where (acct_id,precima_ofb_id) in
	(select acct_id,precima_ofb_id from all_custs_offer_pool_dm1 intersect select acct_id,precima_ofb_id from acct_ofb_check_purch) 
;


/*delete from all_custs_offer_pool where (acct_id,precima_ofb_id) in 
	(select acct_id,precima_ofb_id from all_custs_offer_pool except select acct_id,precima_ofb_id from acct_ofb_check_purch)
	and type='ofb' 
	;
*/

Grant list, select on all_custs_offer_pool to pprcmmrn01_usr_read;


/*************************************************/
/*****DM1 OFFER ASSIGNMENT PROCESS***********/
/*************************************************/

--up to 10 vendor offers
--up to 1 product offer
--rest are offer bank offers


--stack vendor offers with product offers
drop table vendor_prod_dm1;
create temp table vendor_prod_dm1 as
select * from all_custs_offer_pool_dm1_revised where type in ('vendor','product');

--apply super rules for the first time
drop table vendor_prod_super1;
create temp table vendor_prod_super1 as
select * from (select *, row_number()over(partition by acct_id, precima_ofb_id order by priority,v10 desc) as ofb_rank from vendor_prod_dm1)a
where ofb_rank = 1;
--Super Group rule2: 20% within class level
drop table vendor_prod_super2;
create temp table vendor_prod_super2 as
select * from (select *, row_number()over(partition by acct_id, offer_bank_group_code order by priority,v10 desc) as class_rank from vendor_prod_super1)a
where class_rank <= 1;
--Super Group rule3: 33% within class level
drop table vendor_prod_super3;
create temp table vendor_prod_super3 as
select * from (select *, row_number()over(partition by acct_id, offer_bank_supergroup_code order by priority,v10 desc) as super_rank from vendor_prod_super2)a
where super_rank <= 2;

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
drop table vendor_prod_ofb_dm1;
create temp table vendor_prod_ofb_dm1 as
select * from (select * from all_custs_offer_pool_dm1_revised where type='ofb')a union select * from vendor_prod_offer;


--apply super rules
drop table offer_bank_supergroup_code1;
create temp table offer_bank_supergroup_code1 as
select * from (select *, row_number()over(partition by acct_id, precima_ofb_id order by priority,v10 desc) as ofb_rank from vendor_prod_ofb_dm1)a
where ofb_rank = 1;
--Super Group rule2: 20% within class level
drop table offer_bank_supergroup_code2;
create temp table offer_bank_supergroup_code2 as
select * from (select *, row_number()over(partition by acct_id, offer_bank_group_code order by priority,v10 desc) as class_rank from offer_bank_supergroup_code1)a
where class_rank <= 1;
--Super Group rule3: 33% within class level
drop table offer_bank_supergroup_code3;
create temp table offer_bank_supergroup_code3 as
select * from (select *, row_number()over(partition by acct_id, offer_bank_supergroup_code order by priority,v10 desc) as super_rank from offer_bank_supergroup_code2)a
where super_rank <= 2;


--pick the final 32 offers


drop table  offer_assignment_final_dm1;
create table offer_assignment_final_dm1 as
select *,'DM' as channel from (select *, row_number()over(partition by acct_id order by priority, v10 desc) as final_rank from offer_bank_supergroup_code3)a
where final_rank <= 8;

alter table offer_assignment_final_dm1 drop column ofb_rank,class_rank,super_rank,final_rank restrict;

--customers with fewer than 32 offers are not qualified
delete from offer_assignment_final_dm1 where acct_id in 
	(select acct_id from (select acct_id, count(*) as offer_cnt from offer_assignment_final_dm1 group by 1 having offer_cnt < 8)a);



/*************************************************/
/*******Step ?: join with member table************/
/*********transfer from CARD to CUSTOMER**********/
/*************************************************/

drop table temp8_assignment:user;
/*create temp table temp8_assignment:user as
select b.cust_acct_key,a.* from
offer_assignment_final_dm1 a
left join
universe_custs_msn_:user b -- This table is created in part 1
on a.acct_id=b.card_nbr_hash
where b.cust_acct_key not in 
(select distinct cust_acct_key
	from
		(select cust_acct_key,count(distinct card_nbr_hash) as custs from universe_custs_msn_:user group by 1) a 
	where custs>1) 
--and b.more_card_status = 'Registered' -- this is taken care of in the universe_cust_msn creation
;*/
create temp table temp8_assignment:user as
select a.* from
offer_assignment_final_dm1 a
left join
universe_custs_msn_:user b -- This table is created in part 1
on a.acct_id=b.card_nbr_hash
where b.cust_acct_key not in 
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
msn_rd_vendor_offer_table_20160802dyang b
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
(select precima_ofb_id,disc_cap_n_collared,incentive_min,incentive_max,incentive_incrementals 
	from msn_rd_offer_bank_table_20160802rsankaranarayanan --:final_offer_bank_table:user 
	group by 1,2,3,4,5) b
on a.precima_ofb_id=b.precima_ofb_id;




drop table offer_incentive_final_allocations_dm;
create table offer_incentive_final_allocations_dm as
select *,
case when a.precima_ofb_id in ('MOR-26','MOR-89','MOR-62','MOR-67','MOR-11','MOR-86') and ((incentive_tmp*1000)<incentive_min or (incentive_tmp*1000)=incentive_min) then 1.0*incentive_min/1000 
when a.precima_ofb_id in ('MOR-26','MOR-89','MOR-62','MOR-67','MOR-11','MOR-86') and ((incentive_tmp*1000)>incentive_max or (incentive_tmp*1000)=incentive_max) then 1.0*incentive_max/1000
when a.precima_ofb_id not in ('MOR-26','MOR-89','MOR-62','MOR-67','MOR-11','MOR-86') and (incentive_tmp<incentive_min or incentive_tmp=incentive_min) then incentive_min 
when a.precima_ofb_id not in ('MOR-26','MOR-89','MOR-62','MOR-67','MOR-11','MOR-86') and (incentive_tmp>incentive_max or incentive_tmp=incentive_max) then incentive_max
else incentive_tmp end as incentive_final
from temp_all_incentive_union_1 a;
--apply more rules here if available (max, min, etc)


/*********************************************************************************/
/*************************ADJUST INCENTIVE TYPE***********************************/
/*********************************************************************************/

--change point based incentive_max and min to pound based
drop table offer_incentive_final_allocations_dm1_0;
create temp table offer_incentive_final_allocations_dm1_0 as
select *, case when precima_ofb_id in  ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67') then incentive_max/1000 else incentive_max end as inc_max_tmp,
case when precima_ofb_id in  ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67') then incentive_min/1000 else incentive_min end as inc_min_tmp
from offer_incentive_final_allocations_dm;

/*first column: adjusted incentive rate*/

drop table offer_incentive_final_allocations_dm1_1;
create temp table offer_incentive_final_allocations_dm1_1 as
select a.*, round(b.incentive_1,2) as incentive_1,round(b.incentive_2,2) as incentive_2, round(b.incentive_3,2) as incentive_3,b.pe,b.avg_amt
from offer_incentive_final_allocations_dm1_0 a
left join (select cust_acct_key, ofb_id, pe, avg_amt, incentive_1,incentive_2,incentive_3 from garcia_cust_ofb_incentive_lift_new group by 1,2,3,4,5,6,7) b
on a.cust_acct_key=b.cust_acct_key and a.precima_ofb_id=b.ofb_id;

drop table offer_incentive_final_allocations_dm1_2;
create table offer_incentive_final_allocations_dm1_2 as
select *,
case when incentive_tmp <= inc_min_tmp then inc_min_tmp when incentive_tmp >= inc_max_tmp then inc_max_tmp when incentive_tmp is null then inc_min_tmp else incentive_tmp end as inc_adjusted,
case when
(inc_adjusted != inc_min_tmp and inc_adjusted!= incentive_1 and inc_adjusted!= incentive_2  and inc_adjusted != inc_max_tmp )
-- and inc_adjusted != incentive_3 and inc_adjusted!= incentive_2  ) 
then (inc_adjusted - 0.05) end as inc_lower, inc_lower + 0.1 as inc_upper,
case when pe >= -6 and pe < -1 then inc_upper when pe >= -1 and pe <= -0.5 then inc_lower end as inc_bound_adjusted,
case when inc_bound_adjusted is null then inc_adjusted else inc_bound_adjusted end as inc_bound_final
from  offer_incentive_final_allocations_dm1_1 where incentive_3 is null
union all
select *,
case when incentive_tmp <= inc_min_tmp then inc_min_tmp when incentive_tmp >= inc_max_tmp then inc_max_tmp when incentive_tmp is null then inc_min_tmp else incentive_tmp end as inc_adjusted,
case when
(inc_adjusted != inc_min_tmp and inc_adjusted!= incentive_1 and inc_adjusted!= incentive_2 and inc_adjusted != incentive_3 and inc_adjusted != inc_max_tmp )
--  and inc_adjusted!= incentive_2  ) 
then (inc_adjusted - 0.05) end as inc_lower, inc_lower + 0.1 as inc_upper,
case when pe >= -6 and pe < -1 then inc_upper when pe >= -1 and pe <= -0.5 then inc_lower end as inc_bound_adjusted,
case when inc_bound_adjusted is null then inc_adjusted else inc_bound_adjusted end as inc_bound_final
from  offer_incentive_final_allocations_dm1_1 where incentive_3 is not null; 


Grant list, select on offer_incentive_final_allocations_dm1_2 to pprcmmrn01_usr_read;



delete from offer_incentive_final_allocations_dm1_2 where cust_acct_key in
 (select cust_acct_key from offer_incentive_final_allocations_dm1_2 where pe is null);



/*************************************************/
/****************** Budget Allocation*************/
/*************************************************/



--all_custs_part7_priority  3514452 customers in total, using Neo's old table

drop table offer_incentive_final_allocations_dm1_2_mail; --3,703,441
create temp table offer_incentive_final_allocations_dm1_2_mail as
select * from offer_incentive_final_allocations_dm1_2 where mail_opt_in_ind<>0;

--Grant list, select on offer_incentive_final_allocations_dm1_2_mail to pprcmmrn01_usr_read ;


--vendor customer offer assgmt
drop table Vendor_custs_part1;  --537,390
create temp table Vendor_custs_part1 as
select * from
offer_incentive_final_allocations_dm1_2_mail 
where cust_acct_key in (select cust_acct_key from offer_incentive_final_allocations_dm1_2_mail where type='vendor');


-- sampling universe: non vendor customers, without null potential spend customers
drop table sampling_universe_tmp;  --871,465
create temp table sampling_universe_tmp as
select distinct cust_acct_key, acct_id from offer_incentive_final_allocations_dm1_2_mail
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
select cust_acct_key,potential_spend_segment from dy_sample_result_dm1;


-- temp cust list
drop table priority_custs_list_dm1;
create temp table priority_custs_list_dm1 as  --
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








----------------why item 137 appeared here




drop table offer_incentive_final_allocations_dm1_2_mail_2;
create table offer_incentive_final_allocations_dm1_2_mail_2 as
select *, case when priority_custs_tmp is null then 12 else priority_custs_tmp end as priority_custs 
from
(select b.priority_custs_tmp, b.cust_category, a.* from
offer_incentive_final_allocations_dm1_2_mail a
left join
priority_custs_list_dm1 b
on a.cust_acct_key=b.cust_acct_key)a;



select mail_opt_in_ind,priority_custs,cust_category,count(distinct cust_acct_key) as cnt from offer_incentive_final_allocations_dm1_2_mail_2 group by 1,2,3 order by 1,2;


---------------Final Cust List for DM1

 
drop table priority_custs_list_dm1_final;
create table priority_custs_list_dm1_final as
select mail_opt_in_ind, priority_custs,cust_category,cust_acct_key from offer_incentive_final_allocations_dm1_2_mail_2 group by 1,2,3,4;


Grant list, select on priority_custs_list_dm1_final to pprcmmrn01_usr_read;





/*************************************************/
/*************  Final assignment decisions********/
/*************************************************/


drop table final_custs_list_dm1;
create table final_custs_list_dm1 as
--select * from priority_custs_list_dm1_final where priority_custs in (1,2,3,4,5,6,7);  --1,941,351 customers
select * from priority_custs_list_dm1_final where priority_custs in (1,2,3,4,5,6,7,8,9);  --2,621,567 customers


drop table final_offer_assgmt_table_dm1;
create table final_offer_assgmt_table_dm1 as
select *, case when incentive_type='Points' then round((inc_bound_final * 1000),0) else round(inc_bound_final,2) end as incentive_print
from
(select a.*,c.incentive_type from 
offer_incentive_final_allocations_dm1_2_mail_2 a,
final_custs_list_dm1 b,
msn_campaign_offer_bank_ty_incentive c
where 
a.cust_acct_key=b.cust_acct_key
and
a.precima_ofb_id=c.precima_ofb_id)a;



/*************************************************/
/******************Window Placement***************/
/*************************************************/

drop table vendor_cnt;
create temp table vendor_cnt as
select a.*, case when vendor_cnt is not null then vendor_cnt else 0 end as vendor_cnt 
from 
(select *, row_number()over(partition by cust_acct_key, type order by v10 desc) as type_rank from final_offer_assgmt_table_dm1) a
left join
(select cust_acct_key, count(*) as vendor_cnt from offer_incentive_final_allocations_dm1_2_mail_2 where type='vendor' group by 1)b
on a.cust_acct_key=b.cust_acct_key;



drop table final_offer_assgmt_window_dm1;
create table final_offer_assgmt_window_dm1 as
select *, case
when type='vendor' and type_rank = 1 then 1
when type='vendor' and type_rank = 2 then 2
when type='vendor' and type_rank = 3 then 2
when type='vendor' and type_rank = 4 then 2
when type='ofb' and type_rank = 1 then 1
when type='ofb' and type_rank = 2 and vendor_cnt=0 then 1 
when type='ofb' and type_rank = 2 and vendor_cnt in (1,2,3) then 2
when type='ofb' and type_rank = 2 and vendor_cnt = 4 then 3
when type='ofb' and type_rank = 3 and vendor_cnt in (0,1,2) then 2 
when type='ofb' and type_rank = 3 and vendor_cnt in (3,4) then 3
when type='ofb' and type_rank = 4 and vendor_cnt in (0,1) then 2 
when type='ofb' and type_rank = 4 and vendor_cnt in (2,3) then 3
when type='ofb' and type_rank = 5 and vendor_cnt = 0 then 2
when type='ofb' and type_rank = 5 and vendor_cnt in (1,2) then 3
when type='product' then 3 else 3 end as window,
case
when window=1 then '2016-10-31' 
when window=2 then '2016-11-21'
when window=3 then '2016-12-12' end as coupon_start_dt,
case
when window=1 then '2016-11-20'
when window=2 then '2016-12-11'
when window=3 then '2017-01-01' end as coupon_end_dt,
row_number()over(partition by acct_id order by window, priority_tmp, rank) as offer_position_nbr 
from  
(select *, case when priority=1 then 1 when priority=2 then 3 when priority=3 then 2 end as priority_tmp 
	  from vendor_cnt)a
where cust_acct_key in 
(select cust_acct_key from mrsn_sbo_assignment_2016Xmas_50_neox where basket_offer_type!='non-collector');
--(select cust_acct_key from qa_final_offer_assgmt_table_dm1);
--;
--since neo is doing new table ... use a temporary table 




Grant list, select on final_offer_assgmt_window_dm1 to pprcmmrn01_usr_read;


delete from final_offer_assgmt_window_dm1
	where cust_acct_key not in (select cust_acct_key from tc_test_result_dm1);

/*************************************************/
/******************Test Control*******************/
/*************************************************/


drop table final_offer_assgmt_window_dm1_treatment;
create table  final_offer_assgmt_window_dm1_treatment as
select * from final_offer_assgmt_window_dm1 where cust_acct_key in (select cust_acct_key from tc_test_result_dm1 where tc_flag='T');--tc_test_result
Grant list, select on final_offer_assgmt_window_dm1_treatment to pprcmmrn01_usr_read;



/*************************************************/
/******************Rebate Summary*****************/
/*************************************************/
--total

drop table ty_rebate_summary_final_dm;
create table ty_rebate_summary_final_dm as
select b.priority_custs, b.cust_category, a.* from
(select a.*,b.vendor_rebate,c.product_rebate,d.ofb_rebate from
(select cust_acct_key, sum(inc_bound_final) as total_rebate from final_offer_assgmt_window_dm1_treatment group by 1)a
left join
(select cust_acct_key, sum(inc_bound_final) as vendor_rebate from final_offer_assgmt_window_dm1_treatment where type='vendor' group by 1)b
on a.cust_acct_key=b.cust_acct_key
left join
(select cust_acct_key, sum(inc_bound_final) as product_rebate from final_offer_assgmt_window_dm1_treatment where type='product' group by 1)c
on a.cust_acct_key=c.cust_acct_key
left join
(select cust_acct_key, sum(inc_bound_final) as ofb_rebate from final_offer_assgmt_window_dm1_treatment where type='ofb' group by 1)d
on a.cust_acct_key=d.cust_acct_key) a
left join
priority_custs_list_dm1_final b
on a.cust_acct_key=b.cust_acct_key;

Grant list, select on ty_rebate_summary_final_dm to pprcmmrn01_usr_read;


/*************************************************/
/***************************QA********************/
/*************************************************/
-- all in Neo's table. All with collectors.
-- No duplicates in Catalina.
-- All mailed-in
-- Number of vendor offers
/*************************************************/
/******************QA AND SUMMARIES***************/
/*************************************************/
--non promotition offer banks

select * from final_offer_assgmt_window_dm1 where precima_ofb_id in (select precima_ofb_id from non_promote_offer_bank_v1);

--offer bank dist
select a.*,b.offer_bank_name from
(select precima_ofb_id, count(*) as cnt from final_offer_assgmt_freq_catalina_treatment group by 1 )a
left join
:offer_bank_table b
on a.precima_ofb_id=b.precima_ofb_id group by 1,2,3

--QA
select count(distinct cust_acct_key) from final_offer_assgmt_window_dm1;
select cust_acct_key, count(*) as cnt from final_offer_assgmt_window_dm1 group by 1 having cnt !=8;
--cohort
select cohort,count(distinct cust_acct_key) from final_offer_assgmt_window_dm1 group by 1;
--account_number and cust_acct_key
select count(distinct cust_acct_key) from final_offer_assgmt_window_dm1; --2651267
select count(distinct account_number) from final_offer_assgmt_window_dm1; --2651267
select cust_acct_key,count(distinct account_number) as cnt from final_offer_assgmt_window_dm1 group by 1 having cnt > 1;
select account_number,count(distinct cust_acct_key) as cnt from final_offer_assgmt_window_dm1 group by 1 having cnt > 1;
--check null columns
select * from final_offer_assgmt_window_dm1 where inc_bound_final is null;
--check priority groups
select priority_custs,cust_category,count(distinct cust_acct_key) as cnt from final_offer_assgmt_window_dm1 group by 1,2;

--check different offer types
select distinct type from final_offer_assgmt_window_dm1;
select distinct type, priority from final_offer_assgmt_window_dm1;
select distinct precimaofferid from final_offer_assgmt_window_dm1 where type='product';

--check incentive range and incentive type
select * from final_offer_assgmt_window_dm1 where incentive_print > incentive_max or incentive_print < incentive_min;
select * from final_offer_assgmt_window_dm1 where inc_bound_final > inc_max_tmp or incentive_print < inc_min_tmp;

select distinct incentive_type from final_offer_assgmt_window_dm1 where precima_ofb_id in ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67');
select distinct incentive_print from final_offer_assgmt_window_dm1 where precima_ofb_id in ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67');

select distinct incentive_print from final_offer_assgmt_window_dm1;

--cheeck super group rules
select acct_id,item1,count(*) as cnt from final_offer_assgmt_window_dm1 group by 1,2 having cnt >1;
select acct_id,precima_ofb_id,count(*) as cnt from final_offer_assgmt_window_dm1 group by 1,2 having cnt >1;
select acct_id,offer_bank_group_code, count(*) as cnt from final_offer_assgmt_window_dm1 group by 1,2 having cnt >1;
select acct_id,offer_bank_supergroup_code, count(*) as cnt from final_offer_assgmt_window_dm1 group by 1,2 having cnt >2;


--random check
select *  from final_offer_assgmt_window_dm1 where cust_acct_key =178904 and item1 in ('247','2');


--general stats

select priority_custs,cust_category,count(distinct cust_acct_key) from final_offer_assgmt_window_dm1 group by 1,2;

-------------Group Level Rebate Summary

select priority_custs,cust_category,type,
case when type='vendor' then 0 when type='product' then 0.65 when type='ofb' then 0.65 else 1 end as resp_rate,
count(distinct account_number) as custs,
sum(offers) as offers,
sum(rebate) as total_rebate
from
(select priority_custs,cust_category,account_number,type,count(distinct precima_ofb_id) as offers,sum(inc_bound_final) as rebate  
from offer_incentive_final_allocations_union_all_dy_3_mail_2 
group by 1,2,3,4)a
group by 1,2,3,4 order by 1,3;


-------------Cust Level Rebate Summary

--vendor
select cust_acct_key, sum(inc_bound_final) as vendor_rebate from final_offer_assgmt_window_dm1 where type='vendor' group by 1
--product
select cust_acct_key, sum(inc_bound_final) as product_rebate from final_offer_assgmt_window_dm1 where type='product' group by 1
--ofb
select cust_acct_key, sum(inc_bound_final) as ofb_rebate from final_offer_assgmt_window_dm1 where type='ofb' group by 1

