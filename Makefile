dump:
	docker exec -it airflow_workshop-de-pg-cr-af-1 pg_dump -d postgres://jovyan:jovyan@de-pg-cr-af:5432/jovyan --table "stg"."wp_status" --table "stg"."wp_description" --table "stg.up_description" --table "stg"."su_wp" > actual_dump.sql
