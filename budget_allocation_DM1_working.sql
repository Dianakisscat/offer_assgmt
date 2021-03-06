
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


create temp table vertical_pe as
select * from garcia_cust_ofb_incentive_lift_new where (pe between -6 and -1);

create temp table vertical_pe_union as
select distinct cust_acct_key,ofb_id,avg_amt,incentive_min as tmp from vertical_pe union
select distinct cust_acct_key,ofb_id,avg_amt,incentive_1 as tmp from vertical_pe union
select distinct cust_acct_key,ofb_id,avg_amt,incentive_2 as tmp from vertical_pe union
select distinct cust_acct_key,ofb_id,avg_amt,incentive_3 as tmp from vertical_pe union
select distinct cust_acct_key,ofb_id,avg_amt,incentive_max as tmp from vertical_pe;

drop table vertical_pe_max_lift;
create temp table vertical_pe_max_lift as
select distinct cust_acct_key, ofb_id as precima_ofb_id, max(tmp) as inc_optimal
from vertical_pe_union where tmp <= avg_amt
group by 1,2;

drop table dy_garcia_cust_ofb_inc_optimal;
create table dy_garcia_cust_ofb_inc_optimal as
(select *, incentive_min as inc_optimal 
 from garcia_cust_ofb_incentive_lift_new where (pe<-0.5 and pe>-1)  or (incentive_min > avg_amt))     --pe between (-1, -0.5) OR incentive_min > avg_amt
union
(select a.*, b.inc_optimal from
vertical_pe a, vertical_pe_max_lift b															  --pe between (-6,-1)
where a.cust_acct_key=b.cust_acct_key
and a.ofb_id=b.precima_ofb_id);

Grant list, select on dy_garcia_cust_ofb_inc_optimal to pprcmmrn01_usr_read ;


-------------------------------------------------------------------------------
--change point based incentive_max and min to pound based
drop table offer_incentive_final_allocations_union_all_dy_0;
create temp table offer_incentive_final_allocations_union_all_dy_0 as
select *, case when precima_ofb_id in  ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67') then incentive_max/1000 else incentive_max end as inc_max_tmp,
case when precima_ofb_id in  ('MOR-62','MOR-89','MOR-26','MOR-86','MOR-11','MOR-67') then incentive_min/1000 else incentive_min end as inc_min_tmp
from offer_incentive_final_allocations_union_all_dy;
-------------------------------------------------------------------------------


/*first column: adjusted incentive rate*/

drop table offer_incentive_final_allocations_union_all_dy_1;
create temp table offer_incentive_final_allocations_union_all_dy_1 as
select a.*, round(b.incentive_1,2) as incentive_1,round(b.incentive_2,2) as incentive_2, round(b.incentive_3,2) as incentive_3,b.pe,b.avg_amt
from offer_incentive_final_allocations_union_all_dy_0 a
left join (select cust_acct_key, ofb_id, pe, avg_amt, incentive_1,incentive_2,incentive_3 from dy_garcia_cust_ofb_inc_optimal group by 1,2,3,4,5,6,7) b
on a.cust_acct_key=b.cust_acct_key and a.precima_ofb_id=b.ofb_id;



drop table offer_incentive_final_allocations_union_all_dy_2;
create temp table offer_incentive_final_allocations_union_all_dy_2 as
select *,
case when incentive_tmp <= inc_min_tmp then inc_min_tmp when incentive_tmp >= inc_max_tmp then inc_max_tmp when incentive_tmp is null then inc_min_tmp else incentive_tmp end as inc_adjusted,
case when
(inc_adjusted != inc_min_tmp and inc_adjusted!= incentive_1 and inc_adjusted!= incentive_2  and inc_adjusted != inc_max_tmp )
-- and inc_adjusted != incentive_3 and inc_adjusted!= incentive_2  ) 
then (inc_adjusted - 0.05) end as inc_lower, inc_lower + 0.1 as inc_upper,
case when pe between -6 and -1 then inc_upper when pe between -1 and -0.5 then inc_lower end as inc_bound_adjusted,
case when inc_bound_adjusted is null then inc_adjusted else inc_bound_adjusted end as inc_bound_final
from  offer_incentive_final_allocations_union_all_dy_1 where incentive_3 is null
union
select *,
case when incentive_tmp <= inc_min_tmp then inc_min_tmp when incentive_tmp >= inc_max_tmp then inc_max_tmp when incentive_tmp is null then inc_min_tmp else incentive_tmp end as inc_adjusted,
case when
(inc_adjusted != inc_min_tmp and inc_adjusted!= incentive_1 and inc_adjusted!= incentive_2 and inc_adjusted != incentive_3 and inc_adjusted != inc_max_tmp )
--  and inc_adjusted!= incentive_2  ) 
then (inc_adjusted - 0.05) end as inc_lower, inc_lower + 0.1 as inc_upper,
case when pe between -6 and -1 then inc_upper when pe between -1 and -0.5 then inc_lower end as inc_bound_adjusted,
case when inc_bound_adjusted is null then inc_adjusted else inc_bound_adjusted end as inc_bound_final
from  offer_incentive_final_allocations_union_all_dy_1 where incentive_3 is not null; 


/*second column: purely optimal rate based on PE*/

drop table offer_incentive_final_allocations_union_all_dy_3;
create table offer_incentive_final_allocations_union_all_dy_3 as
select *,
case when inc_optimal is null then inc_min_tmp else inc_optimal end as inc_optimal_final
from (
select a.*, b.inc_optimal from 
offer_incentive_final_allocations_union_all_dy_2 a
left join
dy_garcia_cust_ofb_inc_optimal b
on a.cust_acct_key=b.cust_acct_key and a.precima_ofb_id=b.ofb_id)a
;

--NOTICE: A total of of 3576 NULL incentive_final
--select count(*) from offer_incentive_final_allocations_union_all_dy where incentive_final is null

Grant list, select on offer_incentive_final_allocations_union_all_dy_3 to pprcmmrn01_usr_read;



/*************************************************/
/****************** Budget Allocation*************/
/*************************************************/



--all_custs_part7_priority  3514452 customers in total, using Neo's old table

drop table offer_incentive_final_allocations_union_all_dy_3_mail; --3,703,441
create temp table offer_incentive_final_allocations_union_all_dy_3_mail as
select * from offer_incentive_final_allocations_union_all_dy_3 where mail_opt_in_ind<>0;

--Grant list, select on offer_incentive_final_allocations_union_all_dy_3_mail to pprcmmrn01_usr_read ;


--vendor customer offer assgmt
drop table Vendor_custs_part1;  --537,390
create temp table Vendor_custs_part1 as
select * from
offer_incentive_final_allocations_union_all_dy_3_mail 
where cust_acct_key in (select cust_acct_key from offer_incentive_final_allocations_union_all_dy_3_mail where type='vendor');

/*
-- sampling universe: non vendor customers, without null potential spend customers
drop table sampling_universe_tmp;  --3,166,051
create temp table sampling_universe_tmp as
select distinct cust_acct_key, acct_id from offer_incentive_final_allocations_union_all_dy_3_mail
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

*/


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


drop table offer_incentive_final_allocations_union_all_dy_3_mail_2;
create table offer_incentive_final_allocations_union_all_dy_3_mail_2 as
select *, case when priority_custs_tmp is null then 12 else priority_custs_tmp end as priority_custs 
from
(select b.priority_custs_tmp, b.cust_category, a.* from
offer_incentive_final_allocations_union_all_dy_3_mail a
left join
priority_custs_list_dm1 b
on a.cust_acct_key=b.cust_acct_key)a;

---------------Final Cust List for DM1

drop table priority_custs_list_dm1_final;
create table priority_custs_list_dm1_final as
select priority_custs,cust_category,cust_acct_key from offer_incentive_final_allocations_union_all_dy_3_mail_2 group by 1,2,3;


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
(select b.priority_custs,b.cust_category,a.*,c.incentive_type from 
offer_incentive_final_allocations_union_all_dy_3 a,
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
(select cust_acct_key, count(*) as vendor_cnt from offer_incentive_final_allocations_union_all_dy_3_mail_2 where type='vendor' group by 1)b
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
when window=1 then '10/31/2016' 
when window=2 then '11/21/2016'
when window=3 then '12/12/2016' end as BARCODE_START_DATE,
case
when window=1 then '11/20/2016'
when window=2 then '12/11/2016'
when window=3 then '1/1/2017' end as BARCODE_END_DATE
from vendor_cnt where cust_acct_key in 
(select cust_acct_key from mrsn_sbo_assignment_2016Xmas_50_neox where basket_offer_type!='non-collector');
--(select cust_acct_key from qa_final_offer_assgmt_table_dm1);
--;
--since neo is doing new table ... use a temporary table 


Grant list, select on final_offer_assgmt_window_dm1 to pprcmmrn01_usr_read;



/*************************************************/
/**************DELETE WRONG OFFERS****************/
/*************************************************/


------------Correct prod_hierarchky_key under those suspicious offer banks
drop table dy_check_ofb_keys;
create table dy_check_ofb_keys as
select * from dy_offer_bank_revised where precima_ofb_id in (select precima_ofb_id from dy_wrong_offer_banks_v2);

------------Check offer pool
drop table check_acct_ofb_purch;  --737,617 customers purchased from these suspicious offer banks
create temp table check_acct_ofb_purch as
select a.acct_id, a.precima_ofb_id, b.prod_hierarchy_key 
from final_offer_assgmt_window_dm1 a,  dy_check_ofb_keys b
where a.precima_ofb_id=b.precima_ofb_id group by 1,2,3;

------------check if they purchased any of the correct prod_hier_keys
drop table acct_ofb_cust_tlog;
create temp table acct_ofb_cust_tlog as 
select  a.acct_id, a.tran_id,a.tran_dt
from :cust_tlog_table a, check_acct_ofb_purch b           --pull cust tlog hist, get tran_id
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
from acct_ofb_prod_hier a, dy_check_ofb_keys b, final_offer_assgmt_window_dm1 c
where a.prod_hierarchy_key=b.prod_hierarchy_key                  --filter, keep correct prod_hier_keys in our check list
and a.acct_id=c.acct_id and b.precima_ofb_id=c.precima_ofb_id;   --filter, make sure customers are assigned offer for a certain category

select count(distinct acct_id) from acct_ofb_check_purch; --610,165 customers actually purchased from the cagetory that they are assigned an offer


drop table delete_custs;
create table delete_custs as
select distinct cust_acct_key from vendor_cnt where cust_acct_key in (select cust_acct_key from mrsn_sbo_assignment_2016Xmas_50_neox where basket_offer_type!='non-collector')  --2010481
except
select distinct cust_acct_key from final_offer_assgmt_window_dm1;


--Delete those assigned wrong offers 
delete from final_offer_assgmt_window_dm1 where acct_id in 
	(select acct_id from check_acct_ofb_purch except select acct_id from acct_ofb_check_purch);

Grant list, select on final_offer_assgmt_table_dm1 to pprcmmrn01_usr_read;

--TREATMENT GROUPS
drop table final_offer_assgmt_window_dm1_treatment;
create table  final_offer_assgmt_window_dm1_treatment as
select * from final_offer_assgmt_window_dm1 where cust_acct_key in (select cust_acct_key from msn_test_control_DM1 where tc_flag='T'--tc_test_result
Grant list, select on final_offer_assgmt_window_dm1_treatment to pprcmmrn01_usr_read;

/*******************************************************/
/******************Check Wrong Offer Again**************/
/*******************************************************/

------------Correct prod_hierarchky_key under those suspicious offer banks
create table dy_check_ofb_keys_v2 as
select * from :offer_bank_table a, ella_wrong_keys b
--select * from dy_offer_bank_revised a, ella_wrong_keys b
where a.precima_ofb_id=b.pma_offer_code and promoted_flag!='N';


------------Check offer pool
drop table check_acct_ofb_purch;  -- customers purchased from these suspicious offer banks
create temp table check_acct_ofb_purch as
select a.acct_id, a.precima_ofb_id 
from final_offer_assgmt_window_dm1_treatment a,  dy_check_ofb_keys_v2 b
where a.precima_ofb_id=b.precima_ofb_id group by 1,2;

------------check if they purchased any of the correct prod_hier_keys
drop table acct_ofb_cust_tlog;
create temp table acct_ofb_cust_tlog as 
select  a.acct_id, a.tran_id,a.tran_dt
from :cust_tlog_table a, check_acct_ofb_purch b           --pull cust tlog hist, get tran_id
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
from acct_ofb_prod_hier a, dy_check_ofb_keys_v2 b, final_offer_assgmt_window_dm1_treatment c
where a.prod_hierarchy_key=b.prod_hierarchy_key                  --filter, keep correct prod_hier_keys in our check list
and a.acct_id=c.acct_id and b.precima_ofb_id=c.precima_ofb_id;   --filter, make sure customers are assigned offer for a certain category

create table delete_custs_v2 as
select distinct cust_acct_key
from final_offer_assgmt_window_dm1_treatment where acct_id in 
	(select acct_id from(
	(select acct_id,precima_ofb_id from check_acct_ofb_purch      --assumed purchased from offer assignment table
		except 
	 select acct_id,precima_ofb_id from acct_ofb_check_purch))a ) --actually purchased
	;    


Grant list, select on delete_custs_v2 to pprcmmrn01_usr_read;


--Delete those assigned wrong offers 
delete from final_offer_assgmt_window_dm1_treatment where acct_id in 
	(select acct_id from(
	(select acct_id,precima_ofb_id from check_acct_ofb_purch      --assumed purchased from offer assignment table
		except 
	 select acct_id,precima_ofb_id from acct_ofb_check_purch))a ) --actually purchased
	;    



/*************************************************/
/******************QA AND SUMMARIES***************/
/*************************************************/




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
--priority_custs	cust_category	cnt
--1					vendor		537390
--2					sample		306161
--3					HH			123917
--4					HM			224676
--5					HL			157071
--6					MH			207709
--7					MM			384427
--8					ML			262423
--9					LH			447493

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
--priority_custs	cust_category	count
--1	vendor	537390
--2	sample	306161
--3	HH	123917
--4	HM	224676
--5	HL	157071
--6	MH	207709
--7	MM	384427
--8	ML	262423
--9	LH	447493


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


--total

drop table ty_rebate_summary_final;
create table ty_rebate_summary_final as
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

Grant list, select on ty_rebate_summary_final to pprcmmrn01_usr_read;