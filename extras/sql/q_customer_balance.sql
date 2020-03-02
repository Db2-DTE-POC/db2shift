-------------------------------------
-- retrieve customer balance for 10 largest customer accounts
-------------------------------------
connect to sample;
set current_schema="DVDEMO";
select custid, balance from accounts
order by balance desc
fetch first 10 rows only;
