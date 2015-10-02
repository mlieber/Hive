# Hive
some complex queries in Hive for an ETL project;

especially the use of the windowing function:

-- Lets only take 1 unique record for each account_id when there are multiple, we only need one .
-- we take whichever:

select T.cm_id, T.customer_id, T.record_id, T.account_id 
from
(
select 
 row_number() over (partition by n.account_id order by n.record_id) as RANK,
 ..
 
 as well as showcasing some of the functions of Hive:
 
