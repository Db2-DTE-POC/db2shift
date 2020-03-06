The Cloud Pak for Data data virtualization scenario is based on a simple database schema of a "stock trading" application. The schema contains the following tables:
- ACCOUNTS: Each customer may have a trading account with a specific balance.
- CUSTOMERS: Contains master data about each customer.
- CUSTOMERS_JSON: Contains master data about each customer in JSON format.
- STOCK_HISTORY: Contains for each stock symbol and each trading day the corresponding high, low, open, close and transaction volume.
- STOCK_SYMBOLS: Contains for each stock symbol the name of the corresponding company.
- STOCK_TRANSACTIONS: Contains information about stock transactions of all customers.

To setup the Data Virtualization demo scenario on DB2:
1) Install Db2
2) Create the sample database:
	su - db2inst1
	db2sampl
3) Clone the "db2shift" Git repository to the home directory
   of user db2inst1:
	git clone https://github.com/Db2-DTE-POC/db2shift.git
3) Create schema DVDEMO:
	su - db2inst1
	cd db2shift/extras/sql
	db2 -tvf dvdemo.ddl
4) Load schema DVDEMO:
	su - db2inst1
	cd db2shift/extras/sql
	db2 -tvf load_dvdemo.ddl
5) Run a demo query from directory db2shift/extras/sql for example "q_customer_balance.sql" (all queries are located in files with postfix ".sql")
	su - db2inst1
	cd db2shift/extras/sql
	db2 -tvf q_customer_balance.sql
