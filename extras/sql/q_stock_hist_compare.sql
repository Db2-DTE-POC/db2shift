-------------------------------------
-- retrieve complete trading history for stock symbols 'IBM','MSFT','AAPL' 
-------------------------------------
connect to sample;
set current_schema="DVDEMO";
select symbol, tx_date, open from stock_history
where symbol in ('IBM','MSFT','AAPL') AND TX_DATE != '2017-12-01'
order by TX_DATE asc;
