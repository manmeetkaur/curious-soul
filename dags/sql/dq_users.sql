select case when count(*) > 0 then false else true end as check
from airflow_test_1.temp_users
where not REGEXP_CONTAINS(email, r"@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")