-------------------------------------
-- retrieve all stock transactions related to customer "100000"
-------------------------------------
connect to sample;
set current_schema="DVDEMO";
select * from stock_transactions 
where custid=100000;
