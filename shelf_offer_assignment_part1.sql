 /**************************************/
/********Step 0: Preparations**********/
/**************************************/


create temp table rank_:cust_item_level_ranking:cohort as
	select *, rank()over(partition by acct_id order by v10 desc) as rank from :cust_item_level_ranking:cohort;


/*********************************************/
/*****Step 1: assign product level offers*****/
/*********************************************/

--Prepare a base table: bring in offer bank id, super group code, vendor id, offer id, etc
drop table shelf_offer_assignment_:cohort:user;
create temp table shelf_offer_assignment_:cohort:user as
select a.*, c.item, d.precima_ofb_id,d.offer_bank_group_code,d.offer_bank_supergroup_code,b.precimavendorid,b.precimaofferid     	--notice: 1 mpc could relate to multiple ITEM
from 
rank_:cust_item_level_ranking:cohort a, 
(select * from :final_vendor_offer_table:user where offer_class='Product') b,												--get vendor id and offer id
(select distinct item, mpc_baofu,prod_hier_id_lvl3,prod_hier_id_lvl2,prod_hier_id_lvl1 from :product_table) c,				--get distinct item - mpc - product key combination from product table
:final_offer_bank_table:user d  																							--get offer bank hierarchy
where 
c.item=b.item
and a.purch_flag=1
and a.item1=c.mpc_baofu
and c.prod_hier_id_lvl3||'-'||c.prod_hier_id_lvl2||'-'||c.prod_hier_id_lvl1=d.prod_hierarchy_key;

--delete repeated offers because of 1 mpc associated with many ITEMs
drop table shelf_offer_assignment_adj_:cohort:user;
create table shelf_offer_assignment_adj_:cohort:user as
select * from 
(select *, row_number()over(partition by acct_id,item1 order by item) as row_tmp from shelf_offer_assignment_:cohort:user)a 	
where row_tmp=1
distribute on (acct_id,item1);



/*************************************************/
/*******Step 2: join with member table************/
/*********transfer from CARD to CUSTOMER**********/
/*************************************************/

drop table temp_shelf_assignment:user;
create temp table temp_shelf_assignment:user as
select b.cust_acct_key, a.* from
shelf_offer_assignment_adj_:cohort:user a
left join
msn_card_hist b -- This table is created in part 1
on a.acct_id=b.card_nbr_hash;

drop table temp1_shelf_assignment:user;
create temp table  temp1_shelf_assignment:user as
select b.account_ID, a.*, b.email_opt_in_ind from  --will report on ACCOUNT_ID
temp_shelf_assignment:user a
left join
msn_member b
on a.cust_acct_key=b.cust_acct_key
where email_opt_in_ind=1
and 
a.cust_acct_key not in 
(select distinct cust_acct_key from
		(select cust_acct_key,count(distinct acct_id) as custs from temp_shelf_assignment:user group by 1) a 
		 where custs>1);


/********************************************************/
/****Step 3: choose the final 3-8 offers to be assigned**/
/********************************************************/

/*
\echo
\echo ' Apply the super group rule 1 - Only one offer per offer bank grp id'
\echo
*/


drop table shelf_super1_:cohort:user;
create table shelf_super1_:cohort:user as
select * 
from 
(select *, row_number()over(partition by acct_id, precima_ofb_id order by v10 desc) as ofb_rank 
from 
temp1_shelf_assignment:user)a
where ofb_rank = 1;

/*
\echo
\echo 'at most 50% of offers from a single super group, i.e. at most 4 offers per super group by now'
\echo
*/

drop table  shelf_super2_:cohort:user;
create table shelf_super2_:cohort:user as
select * 
from 
(select *, row_number()over(partition by acct_id, offer_bank_supergroup_code order by v10 desc) as super_rank 
from 
shelf_super1_:cohort:user)a
where super_rank <= 4;

/*
\echo
\echo 'select the first 8 offers from all potential offers'
\echo
*/
drop table shelf_super3_:cohort:user;
create temp table shelf_super3_:cohort:user as 
select * from 
(select *, count(*)over(partition by acct_id) as total_temp from shelf_super2_:cohort:user)a 
where total_temp <= 8;   							--if we can successfully select 8 from all, then they must not violate the 50% rule


drop table shelf_super4_:cohort:user; 
create table shelf_super4_:cohort:user as
select cust_acct_key, account_id, acct_id,item1,v10,rank,item,precima_ofb_id,offer_bank_group_code,offer_bank_supergroup_code,precimavendorid,precimaofferid from 
(select *, row_number()over(partition by acct_id, offer_bank_supergroup_code order by v10 desc) as super_rank 
	from shelf_super3_:cohort:user )a 				--if 3-7 offers are selected, we have to check again if they violate the 50% rule
where super_rank <= total_temp*0.5;					--offers within a super group must be fewer than 0.5 * total


--customers with fewer than 3 offers are not qualified

drop table shelf_offer_final_:cohort:user;
create table shelf_offer_final_:cohort:user as
select * from shelf_super4_:cohort:user
where acct_id not in 
	(select acct_id from (select acct_id, count(*) as offer_cnt from shelf_super4_:cohort:user group by 1 having offer_cnt < 3)a)
distribute on (acct_id, item1)
	;





