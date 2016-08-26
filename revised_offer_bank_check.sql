------------Prep work
create table dy_offer_bank_revised as 
select * from MSN_campaign_offer_Bank_hist;

create table dy_wrong_offer_banks_v2 as
select cast(pma_offer_code as varchar(25)) as  precima_ofb_id, min_dept_num||'-'||min_class_num||'-'||min_subclass_num as prod_hierarchy_key
from dy_wrong_offer_banks;

------------A NEW REVISED OFFER BANK TABLE, WITHOUT WRONG PROD_HIERARCHY_KEY
delete from dy_offer_bank_revised where prod_hierarchy_key in (select prod_hierarchy_key from dy_wrong_offer_banks_v2);

Grant list, select on dy_wrong_offer_banks_v2 to pprcmmrn01_usr_read ;

Grant list, select on dy_offer_bank_revised to pprcmmrn01_usr_read ;

------------Correct prod_hierarchky_key under those suspicious offer banks
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
 





