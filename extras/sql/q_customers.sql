-------------------------------------
-- retrieve customer data using JSON
-------------------------------------
connect to sample;
set current_schema="DVDEMO";
select RESULTS.* from CUSTOMERS C,
  JSON_TABLE( C.INFO, 'strict $'
  COLUMNS(
	CUSTID INT PATH '$.customerid',
	FIRST_NAME VARCHAR(20) PATH '$.identity.firstname',
	LAST_NAME VARCHAR(20) PATH '$.identity.lastname',
	STATE CHAR(2) PATH '$.contact.state',
	ZIPCODE CHAR(5) PATH '$.contact.zipcode'
	)
	ERROR ON ERROR) AS RESULTS
FETCH FIRST 10 ROWS ONLY;
