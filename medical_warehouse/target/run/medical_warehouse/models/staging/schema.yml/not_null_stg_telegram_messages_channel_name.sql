
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select channel_name
from "medical_warehouse"."staging"."stg_telegram_messages"
where channel_name is null



  
  
      
    ) dbt_internal_test