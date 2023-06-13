select * from dept;

drop table dept purge;


select * from emp;

drop table emp purge;


select * from dulux;

select count(*) from dulux;

drop table dulux purge;


select * from "Car_sales_data"
where rownum <= 100;

call dbms_stats.gather_table_stats(user,'"Car_sales_data"');

select count(*) from "Car_sales_data";

select table_name, num_rows, blocks, blocks*8192/1024/1024 as MB from user_tables where table_name = 'Car_sales_data';

drop table "Car_sales_data" purge;


select * from "Canterbury_corpus";

drop table "Canterbury_corpus" purge;


select * from car_sales_data_ms
where rownum <= 100;

call dbms_stats.gather_table_stats(user,'CAR_SALES_DATA_MS');

select count(*) from car_sales_data_ms;

select table_name, num_rows, blocks, blocks*8192/1024/1024 as MB from user_tables where table_name = 'CAR_SALES_DATA_MS';

drop table car_sales_data_ms purge;
