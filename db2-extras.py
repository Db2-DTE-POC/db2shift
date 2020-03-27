%%sql define LIST
#
# The LIST macro is used to list all of the tables in the current schema or for all schemas
#
var syntax Syntax: LIST TABLES [FOR ALL | FOR SCHEMA name]
# 
# Only LIST TABLES is supported by this macro
#
if {^1} <> 'TABLES'
    exit {syntax}
endif

#
# This SQL is a temporary table that contains the description of the different table types
#
WITH TYPES(TYPE,DESCRIPTION) AS (
  VALUES
    ('A','Alias'),
    ('G','Created temporary table'),
    ('H','Hierarchy table'),
    ('L','Detached table'),
    ('N','Nickname'),
    ('S','Materialized query table'),
    ('T','Table'),
    ('U','Typed table'),
    ('V','View'),
    ('W','Typed view')
)
SELECT TABNAME, TABSCHEMA, T.DESCRIPTION FROM SYSCAT.TABLES S, TYPES T
       WHERE T.TYPE = S.TYPE 

#
# Case 1: No arguments - LIST TABLES
#
if {argc} == 1
   AND OWNER = CURRENT USER
   ORDER BY TABNAME, TABSCHEMA
   return
endif 

#
# Case 2: Need 3 arguments - LIST TABLES FOR ALL
#
if {argc} == 3
    if {^2}&{^3} == 'FOR&ALL'
        ORDER BY TABNAME, TABSCHEMA
        return
    endif
    exit {syntax}
endif

#
# Case 3: Need FOR SCHEMA something here
#
if {argc} == 4
    if {^2}&{^3} == 'FOR&SCHEMA'
        AND TABSCHEMA = '{^4}'
        ORDER BY TABNAME, TABSCHEMA
        return
    else
        exit {syntax}
    endif
endif

#
# Nothing matched - Error
#
exit {syntax}



%%sql define describe
#
# The DESCRIBE command can either use the syntax DESCRIBE TABLE <name> or DESCRIBE TABLE SELECT ...
#
var syntax Syntax: DESCRIBE [TABLE name | SELECT statement] 
#
# Check to see what count of variables is... Must be at least 2 items DESCRIBE TABLE x or SELECT x
#
if {argc} < 2
   exit {syntax}
endif

CALL ADMIN_CMD('{*0}');


