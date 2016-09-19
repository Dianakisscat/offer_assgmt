 /**************************************/
/********Step 0: Preparations**********/
/**************************************/

create temp table rank_:cust_item_level_ranking:cohort as
	select *, rank()over(partition by acct_id order by v10 desc) as rank from :cust_item_level_ranking:cohort;


/*********************************************/
/*****Step 1: assign product level offers*****/
/*********************************************/


drop table shelf_offer_assignment_:cohort:user;
create table shelf_product_offer_assignment_:cohort:user as
select a.*, d.precima_ofb_id,d.offer_bank_group_code,d.offer_bank_supergroup_code,'product' as type,b.precimavendorid,b.precimaofferid, 2 as priority 
from 
rank_:cust_item_level_ranking:cohort a, 
--product item offers
(select * from :final_vendor_offer_table:user where offer_class='Product') b,
:product_table c,
:final_offer_bank_table:user d  --get offer bank hierarchy

where 
c.item=b.item
and a.purch_flag=1
and a.item1=c.mpc_baofu
and c.prod_hier_id_lvl3||'-'||c.prod_hier_id_lvl2||'-'||c.prod_hier_id_lvl1=d.prod_hierarchy_key;



/***************************************************/
/****Decide on the final 3-8 offers to be assigned**/
/***************************************************/