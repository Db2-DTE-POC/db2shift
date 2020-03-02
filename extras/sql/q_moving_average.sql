-------------------------------------
-- retrieve moving average
-------------------------------------
connect to sample;
set current_schema="DVDEMO";
select tx_date, open, avg(open) over (
	order by tx_date
	rows between 15 preceding and 15 following) as moving_avg
from stock_history
where symbol = 'AAPL'
order by tx_date;
