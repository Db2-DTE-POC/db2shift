connect to sample;
IMPORT FROM stock_history.csv OF DEL COMMITCOUNT 10000 INSERT INTO STOCK_HISTORY;
IMPORT FROM accounts.del OF DEL COMMITCOUNT 10000 INSERT INTO ACCOUNTS;
IMPORT FROM stock_symbols.del OF DEL COMMITCOUNT 10000 INSERT INTO STOCK_SYMBOLS;
IMPORT FROM stock_transactions.del OF DEL COMMITCOUNT 10000 INSERT INTO STOCK_TRANSACTIONS;
IMPORT FROM customers.js OF ASC METHOD l(1 2000) INSERT INTO CUSTOMERS(INFO);