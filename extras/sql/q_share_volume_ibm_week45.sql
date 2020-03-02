-------------------------------------
-- retrieve trading volume related to IBM shares from week 45
-------------------------------------
connect to sample;
set current_schema="DVDEMO";
select symbol, day(tx_date), volume/1000000 from stock_history
where symbol in ('IBM','MSFT','AAPL') and week(tx_date)=45
order by day(tx_date) asc;
