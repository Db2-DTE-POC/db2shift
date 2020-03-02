-------------------------------------
-- retrieve price of IBM stock over the year
-------------------------------------
connect to sample;
set current_schema="DVDEMO";
select tx_date, open from stock_history
where symbol = 'IBM' and tx_date != '2017-12-01'
order by tx_date asc;
