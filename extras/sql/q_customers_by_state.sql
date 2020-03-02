-------------------------------------
-- Top customers by state
-------------------------------------

connect to sample;
set current_schema="DVDEMO";

WITH BY_STATE(STATE,CUSTCOUNT) AS (
	SELECT JSON_VALUE(INFO,'$.contact.state' RETURNING CHAR(2)), COUNT(*) FROM CUSTOMERS
	GROUP BY JSON_VALUE(INFO,'$.contact.state' RETURNING CHAR(2))
)
SELECT STATE,CUSTCOUNT FROM BY_STATE
ORDER BY CUSTCOUNT DESC
FETCH FIRST 10 ROWS ONLY;
