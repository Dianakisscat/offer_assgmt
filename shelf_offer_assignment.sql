 /**************************************/
/********Step 0: Preparations**********/
/**************************************/

create temp table rank_:cust_item_level_ranking:cohort as
	select *, rank()over(partition by acct_id order by v10 desc) as rank from :cust_item_level_ranking:cohort;


/*********************************************/
/*****Step 1: assign product level offers*****/
/*********************************************/


drop table shelf_offer_assignment_:cohort:user;
create table shelf_offer_assignment_:cohort:user as

select a.*, d.precima_ofb_id,d.offer_bank_group_code,d.offer_bank_supergroup_code,b.precimavendorid,b.precimaofferid
from 
rank_:cust_item_level_ranking:cohort a, 
--product item offers
(select * from :final_vendor_offer_table:user where offer_class='Product') b,
(select distinct item, mpc_baofu,prod_hier_id_lvl3,prod_hier_id_lvl2,prod_hier_id_lvl1 from :product_table) c,
:final_offer_bank_table:user d  --get offer bank hierarchy

where 
c.item=b.item
and a.purch_flag=1
and a.item1=c.mpc_baofu
and c.prod_hier_id_lvl3||'-'||c.prod_hier_id_lvl2||'-'||c.prod_hier_id_lvl1=d.prod_hierarchy_key;



/********************************************************/
/****Step 2: choose the final 3-8 offers to be assigned**/
/********************************************************/


/*
\echo
\echo ' Apply the rule - Only one offer per offer bank grp id'
\echo
*/


drop table shelf_super1_:cohort:user;
create temp table shelf_super1_:cohort:user as
select * 
from 
(select *, row_number()over(partition by acct_id, precima_ofb_id order by v10 desc) as ofb_rank 
from 
shelf_offer_assignment_:cohort:user)a
where ofb_rank = 1;

/*
\echo
\echo 'Super Group rule2: 50% within super group level'
\echo
*/


drop table shelf_super2_:cohort:user;
create temp table shelf_super2_:cohort:user as
select * from 
(select *, row_number()over(partition by acct_id, offer_bank_supergroup_code order by v10 desc) as super_rank 
from 
(select *, count(*)over(partition by acct_id) as temp_total from shelf_super1_:cohort:user)a
)a
where super_rank <= temp_total*0.5;	

/*
\echo
\echo 'pick the final 3-8 shelf offers by order'
\echo
*/

drop table  final_offer_shelf_:cohort:user;
create table final_offer_shelf_:cohort:user as
select *,'Email' as channel
from 
(select *, row_number()over(partition by acct_id order by v10 desc) as final_rank from shelf_super2_:cohort:user)a
where final_rank <= 8; 	

alter table final_offer_shelf_:cohort:user drop column temp_total,ofb_rank,super_rank,final_rank restrict;

--customers with fewer than 3 offers are not qualified
delete from final_offer_shelf_:cohort:user where acct_id in 
	(select acct_id from (select acct_id, count(*) as offer_cnt from final_offer_shelf_:cohort:user group by 1 having offer_cnt < 3)a);



/*************************************************/
/*******Step 3: join with member table************/
/*********transfer from CARD to CUSTOMER**********/
/*************************************************/

drop table temp_shelf_assignment:user;
create temp table temp_shelf_assignment:user as
select b.cust_acct_key, a.* from
final_offer_shelf_:cohort:user a
left join
msn_card_hist b -- This table is created in part 1
on a.acct_id=b.card_nbr_hash;

drop table temp1_shelf_assignment:user;
create temp table  temp1_shelf_assignment:user as
select b.account_ID, a.*,b.email_opt_in_ind from  --will report on ACCOUNT_ID
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