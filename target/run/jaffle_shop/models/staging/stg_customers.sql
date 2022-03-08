
  create or replace  view SANDBOX.MANMEETRANGOOLA.stg_customers 
  
   as (
    with source as (
    select * from SANDBOX.MANMEETRANGOOLA.raw_customers

),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name

    from source

)

select * from renamed
  );
