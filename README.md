# Hive
some complex queries in Hive for an ETL project;

especially the use of the **Row number** function:

>-- Lets only take 1 unique record for each account_id when there are multiple, we only need one .
>-- we take whichever:

>select T.cm_id, T.customer_id, T.record_id, T.account_id 
>from
>(
>select 
> row_number() over (partition by n.account_id order by n.record_id) as RANK,
> ..
 
 as well as showcasing some of the functions of Hive, **COALESCE, CASE, IF** :
 
> COALESCE(n2.final_spend, opp.potential_annual_spend) as potential_annual_spend,

>IF ( n2.final_spend is not null and opp.frequency is not null AND opp.average_transaction_size is null ,

>   CASE opp.frequency WHEN 'ANNUAL'  THEN n2.final_spend / 1

>                  WHEN 'SEMI-ANNUAL' then n2.final_spend / 2

>                  WHEN 'QUARTERLY' THEN n2.final_spend / 4

>                  WHEN 'BI-MONTHLY' THEN n2.final_spend / 6

>                  WHEN 'MONTHLY' THEN n2.final_spend / 12

>                  WHEN 'BI-WEEKLY' THEN n2.final_spend /  26

>                  WHEN 'WEEKLY' THEN n2.final_spend /  52

>                  WHEN 'DAILY' THEN n2.final_spend /  365 END,

>     opp.average_transaction_size) as average_transaction_size,

>  opp.frequency as frequency,

>opp.outstanding_invoice  as outstanding_invoice,

>opp.urgent_flag as urgent_flag,

>IF (n2.final_spend is not null and CAST(n2.final_spend as DECIMAL) > 250000 AND n2.endorsement_id is not null , 'Yes', 'No') as 
>top_vendor_flag,

>opp.cm_specific_acceptance as cm_specific_acceptance,



 
