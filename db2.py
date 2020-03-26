#
# Set up Jupyter MAGIC commands "sql". 
# %sql will return results from a DB2 select statement or execute a DB2 command
#
# IBM 2018: George Baklarz
# Version 2018-02-26
#

#import sys
#if (len(sys.argv) > 1):
#    if (sys.argv[1] == "-update"):
#        !pip install --user ibm_db
#        !pip install --user --upgrade pixiedust

from __future__ import print_function

# Override the name of display, HTML, and Image in the event you plan to use the pixiedust library for
# rendering graphics.

from IPython.display import HTML as pHTML, Image as pImage, display as pDisplay
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic)
from pixiedust.display import *
from pixiedust.utils.shellAccess import ShellAccess

import ibm_db
import pandas
import ibm_db_dbi
import json
import matplotlib.pyplot as plt
import getpass
import os
import pickle
import time
import sys
import re
import warnings

warnings.filterwarnings("ignore")

# Python Hack for Input between 2 and 3

try: 
    input = raw_input 
except NameError: 
    pass 

##plt.style.use('ggplot')

settings = {
     "maxrows"  : 10,    
     "database" : "",
     "hostname" : "localhost",
     "port"     : "50000",
     "protocol" : "TCPIP",    
     "uid"      : "DB2INST1",
     "pwd"      : "password"
}

connected = False
hdbc = None
hdbi = None
runtime = 1

def load_ipython_extension(ipython):

	# Register the Magic extension in Jupyter 
	   
	ip = ipython # get_ipython()          
	ip.register_magics(Db2)
	load_settings()
	success("DB2 Extensions Loaded.")      

def sqlhelp():
    
    sd = '<td style="text-align:left;">'
    ed = '</td>'
    sh = '<th style="text-align:left;">'
    eh = '</th>'
    sr = '<tr>'
    er = '</tr>'
    
    helpSQL = """
       <h3>SQL Options</h3> 
       <p>The following options are available as part of a SQL statement. The options are always preceded with a
       minus sign (i.e. -q).
       <table>
        {sr}
           {sh}Option{eh}
           {sh}Description{eh}
        {er}
        {sr}
           {sd}a{ed}
           {sd}Return all rows in answer set and do not limit display{ed}
        {er}       
        {sr}
          {sd}d{ed}
          {sd}Change SQL delimiter to "@" from ";"{ed}
        {er}
        {sr}
          {sd}h{ed}
          {sd}Display %sql help information.{ed}
        {er}        
        {sr}
          {sd}i{ed}
          {sd}Return the data in a pixiedust display to view the data and optionally plot it.{ed}
        {er}  
        {sr}
          {sd}j{ed}
          {sd}Create a pretty JSON representation. Only the first column is formatted{ed}
        {er} 
        {sr}
          {sd}n{ed}
          {sd}Execute all of the SQL as commands rather than select statements (no answer sets){ed}
        {er}   
        {sr}
          {sd}pb{ed}
          {sd}Plot the results as a bar chart{ed}
        {er}
        {sr}
          {sd}pl{ed}
          {sd}Plot the results as a line chart{ed}
        {er}
        {sr}
          {sd}pp{ed}
          {sd}Plot Pie: Plot the results as a pie chart{ed}
        {er}        
        {sr}
          {sd}q{ed}
          {sd}Quiet results - no answer set or messages returned from the function{ed}
        {er}
        {sr}  
          {sd}r{ed}
          {sd}Return the result set as an array of values{ed}
        {er}
        {sr}
          {sd}s{ed}
          {sd}Execute everything as SELECT statements. By default, SELECT, VALUES, and WITH are considered part of an answer set, but it is possible that you have an SQL statement that does not start with any of these keywords but returns an answer set.
          {ed} 
        {er}
        {sr}
          {sd}sampledata{ed}
          {sd}Create and load the EMPLOYEE and DEPARTMENT tables{ed}
        {er}        
        {sr}
          {sd}t{ed}
          {sd}Time the following SQL statement and return the number of times it executes in 1 second{ed}
        {er}
       </table>
       """
    
    helpSQL = helpSQL.format(**locals())
        
    pDisplay(pHTML(helpSQL))
    
def connected_help():
    
    sd = '<td style="text-align:left;">'
    ed = '</td>'
    sh = '<th style="text-align:left;">'
    eh = '</th>'
    sr = '<tr>'
    er = '</tr>'
        
    helpConnect = """
       <h3>Connecting to DB2</h3> 
       <p>The CONNECT command has the following format:
       <pre>
       %sql CONNECT TO &lt;database&gt; USER &lt;userid&gt; USING &lt;password|?&gt; HOST &lt;ip address&gt; PORT &lt;port number&gt;
       </pre>
       If you use a "?" for the password field, the system will prompt you for a password. This avoids typing the 
       password as clear text on the screen. If a connection is not successful, the system will print the error
       message associated with the connect request.
       <p>
       Note: When prompted for input, you can use the format ip:port or #x:port where #x represents 
       the last digits of the containers IP address. For instance, if the Db2 server is found on 172.17.0.2 then
       you would use the value #2 to represent 172.17.0.2:50000. 
       <p>
       If the connection is successful, the parameters are saved on your system and will be used the next time you
       run an SQL statement, or when you issue the %sql CONNECT command with no parameters.
       <p>If you issue CONNECT RESET, all of the current values will be deleted and you will need to 
       issue a new CONNECT statement. 
       <p>A CONNECT command without any parameters will attempt to re-connect to the previous database you 
       were using. If the connection could not be established, the program to prompt you for
       the values. To use the default values, just hit return for each input line. The default values are: 
       <table>
       {sr}
         {sh}Setting{eh}
         {sh}Description{eh}
       {er}
       {sr}
         {sd}Database{ed}{sd}SAMPLE{ed}
       {er}
       {sr}
         {sd}Hostname{ed}
         {sd}The default is localhost, but usually you need to supply the host name or IP address. If you are running in a Docker environment you can specify the host IP address
             with just #x:yyyy where x represents the last number of the Docker container address (which is usually
             172.17.0.x). If you do not supply the port number, the default will be 50000.{ed}
       {er}
       {sr}
         {sd}PORT{ed}
         {sd}50000 (the default Db2 port){ed}
       {er}
       {sr}         
         {sd}Userid{ed}
         {sd}DB2INST1{ed} 
       {er}
       {sr}                  
         {sd}Password{ed}
         {sd}No password is provided so you have to enter a value{ed}
       {er}
       {sr}       
         {sd}Maximum Rows{ed}
         {sd}10 lines of output are displayed when a result set is returned{ed}
       {er}
       </table>
       """
    
    helpConnect = helpConnect.format(**locals())
        
    pDisplay(pHTML(helpConnect))  
    
# Prompt for Connection information

def connected_prompt():
    
    global settings
    
    settings["database"] = input("Enter the database name [SAMPLE]: ") or "SAMPLE";
    hostport = input("Enter the HOST IP address and PORT in the form ip:port or #x:port [localhost:50000].") or "localhost:50000";
    ip, port = split_ipport(hostport)
    if (port == None): port = "50000"
    settings["hostname"] = ip
    settings["port"]     = port
    settings["uid"]      = input("Enter Userid on the DB2 system [DB2INST1]: ").upper() or "DB2INST1";
    settings["pwd"]      = getpass.getpass("Password [password]: ") or "password";
    settings["maxrows"]  = input("Maximum rows displayed [10]: ") or "10";
    settings["maxrows"]  = int(settings["maxrows"])
    
# Split port and IP addresses

def split_ipport(in_port):

    # Split input into an IP address and Port number

    checkports = in_port.split(':')
    ip = checkports[0]
    if (len(checkports) > 1):
        port = checkports[1]
    else:
        port = None

    if (ip[:1] == '#'):
        ip = "172.17.0." + ip[1:]

    return ip, port
    
# Parse the CONNECT statement and execute if possible 

def parseConnect(inSQL):
    
    global settings, connected

    connected = False
    
    cParms = inSQL.split()
    cnt = 0
    
    while cnt < len(cParms):
        if cParms[cnt].upper() == 'TO':
            if cnt+1 < len(cParms):
                settings["database"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No database specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'USER':
            if cnt+1 < len(cParms):
                settings["uid"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No userid specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'USING':
            if cnt+1 < len(cParms):
                settings["pwd"] = cParms[cnt+1]   
                if (settings["pwd"] == '?'):
                    settings["pwd"] = getpass.getpass("Password [password]: ") or "password"
                cnt = cnt + 1
            else:
                errormsg("No password specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'HOST':
            if cnt+1 < len(cParms):
                hostport = cParms[cnt+1].upper()
                ip, port = split_ipport(hostport)
                if (port == None): settings["port"] = "50000"
                settings["hostname"] = ip
                cnt = cnt + 1
            else:
                errormsg("No hostname specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'PORT':                           
            if cnt+1 < len(cParms):
                settings["port"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No port specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'RESET': 
             settings["database"] = ''
             success("Connection reset.")
             return
        else:
            cnt = cnt + 1
                     
    db2_doConnect()
    
# Connect to DB2 and prompt if you haven't set any of the values yet

def db2_doConnect():
    
    global hdbc, hdbi, connected, runtime
    global settings  

    if connected == False: 
        
        if len(settings["database"]) == 0:
            connected_help()
            connected_prompt()
    
    dsn = (
           "DRIVER={{IBM DB2 ODBC DRIVER}};"
           "DATABASE={0};"
           "HOSTNAME={1};"
           "PORT={2};"
           "PROTOCOL=TCPIP;"
           "UID={3};"
           "PWD={4};").format(settings["database"], settings["hostname"], settings["port"], settings["uid"], settings["pwd"])

    # Get a database handle (hdbc) and a statement handle (hstmt) for subsequent access to DB2

    try:
        hdbc  = ibm_db.connect(dsn, "", "")
    except Exception as err:
        errormsg(str(err))
        connected = False
        settings["database"] = ''
        return
    
    try:
        hdbi = ibm_db_dbi.Connection(hdbc)
    except Exception as err:
        errormsg(str(err))
        connected = False
        settings["database"] = ''
        return        
    
    connected = True
    
    # Save the values for future use
    
    save_settings()
    
    success("Connection successful.")
        
def load_settings():

    # This routine will load the settings from the previous session if they exist
    
    global settings
    
    fname = "db2connect.pickle"

    try:
        with open(fname,'rb') as f: 
            settings = pickle.load(f) 
            
    except: 
        pass
    
    return

def save_settings():

    # This routine will save the current settings if they exist
    
    global settings
    
    fname = "db2connect.pickle"
    
    try:
        with open(fname,'wb') as f:
            pickle.dump(settings,f)
            
    except:
        errormsg("Failed trying to write DB2 Configuration Information.")
 
    return  
    
# Print out the DB2 error generated by the last executed statement

def db2_error(quiet):
    
    if quiet == True: return

    html = '<p style="border:2px; border-style:solid; border-color:#FF0000; background-color:#ffe6e6; padding: 1em;">'

    errmsg = ibm_db.stmt_errormsg().replace('\r',' ')
    errmsg = errmsg[errmsg.rfind("]")+1:].strip()
    pDisplay(pHTML(html+errmsg+"</p>"))
    
# Print out an error message

def errormsg(message):
    
    if (message != ""):
        html = '<p style="border:2px; border-style:solid; border-color:#FF0000; background-color:#ffe6e6; padding: 1em;">'
        pDisplay(pHTML(html + message + "</p>"))     
    
def success(message):
    
    if (message != ""):
        print(message)
#
#    Optional Code to print green border around success statements
#
#    html = '<p style="border:2px; border-style:solid; border-color:#008000; background-color:#e6ffe6; padding: 1em;">'
#            
#    if (message != ""):
#        pDisplay(pHTML(html + message + "</p>"))
        
    return     
    
# Run a command for one second to see how many times we execute it and return the count

def sqlTimer(hdbc, runtime, inSQL, parms):
    
    count = 0
    t_end = time.time() + runtime
    
    while time.time() < t_end:
        
        try:
            if (len(parms) > 0):
                stmt = ibm_db.prepare(hdbc,inSQL)
                rc = ibm_db.execute(stmt,tuple(parms))
                if (rc == False):
                    print("SQL Execution error during timing.")
                    return(-1)
            else:
                stmt = ibm_db.exec_immediate(hdbc,inSQL) 
                
            ibm_db.free_result(stmt)
            
        except Exception as err:
            print(err)
            db2_error(False)
            return(-1)
        
        count = count + 1
                    
    return(count)
    
def sqlParser(sqlin):
       
    sql_cmd = ""
    parameter_list = []
    encoded_sql = sqlin
    
    firstCommand = "(?:^\s*)([a-zA-Z]+)(?:\s+.*|$)"
    
    findFirst = re.match(firstCommand,sqlin)
    
    if (findFirst == None): # We did not find a match so we just return the empty string
        return sql_cmd, parameter_list, encoded_sql
    
    cmd = findFirst.group(1)
    sql_cmd = cmd.upper()

    #
    # Scan the input string looking for variables in the format :var. If no : is found just return.
    # Var must be alpha+number+_ to be valid
    #
    
    if (':' not in sqlin): # A quick check to see if parameters are in here, but not fool-proof!         
        return sql_cmd, parameter_list, encoded_sql    
    
    quoteChar = ""
    inQuote = False
    inVar = False 
    varName = ""
    encoded_sql = ""
    
    for ch in sqlin:
        if (inVar == True): # We are collecting the name of a variable
            if (ch.upper() in "_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"):
                varName = varName + ch
                continue
            else:
                parameter_list.append(getContents(varName))
                varName = ""
                inVar = False
               
        if (ch == "\"" or ch == "\'"): # Do we have a quote
            if (quoteChar == ""):
                quoteChar = ch
                inQuote = True
            else:
                if (quoteChar == ch): # Check which quote we found to avoid quote in quote situations
                    inQuote = False
                    quoteChar = ""
            encoded_sql = encoded_sql + ch 
        elif (ch == ":" and inQuote == False): # This might be a variable
             varName = ""
             inVar = True
             encoded_sql = encoded_sql + "?"
        else:
             encoded_sql = encoded_sql + ch
        
    # We close a quoted string if you forgot it
    
    if (inQuote == True):
        encoded_sql = encoded_sql + quoteChar
        
    if (inVar == True):
        parameter_list.append(getContents(varName))
        
    return sql_cmd, parameter_list, encoded_sql
    
def getContents(varName):
    
    #
    # Get the contents of the variable name that is passed to the routine
    #
    
    varValue = None
    
    if (varName in globals()): # Does the variable exist?
        temp_global = globals().copy()
        varValue = temp_global[varName]
   
    return(varValue)
    
def db2_create_sample(quiet):

    if (connected == False):                                      # Check if you are connected 
        db2_doConnect()
        if connected == False: return
    
    create_department = """
      BEGIN
        DECLARE FOUND INTEGER; 
        SET FOUND = (SELECT COUNT(*) FROM SYSIBM.SYSTABLES WHERE NAME='XDEPARTMENT' AND CREATOR=CURRENT USER); 
        IF FOUND = 0 THEN 
           EXECUTE IMMEDIATE('CREATE TABLE XDEPARTMENT(DEPTNO CHAR(3) NOT NULL, DEPTNAME VARCHAR(36) NOT NULL, 
                              MGRNO CHAR(6),ADMRDEPT CHAR(3) NOT NULL)'); 
           EXECUTE IMMEDIATE('INSERT INTO XDEPARTMENT VALUES 
             (''A00'',''SPIFFY COMPUTER SERVICE DIV.'',''000010'',''A00''), 
             (''B01'',''PLANNING'',''000020'',''A00''), 
             (''C01'',''INFORMATION CENTER'',''000030'',''A00''), 
             (''D01'',''DEVELOPMENT CENTER'',NULL,''A00''), 
             (''D11'',''MANUFACTURING SYSTEMS'',''000060'',''D01''), 
             (''D21'',''ADMINISTRATION SYSTEMS'',''000070'',''D01''), 
             (''E01'',''SUPPORT SERVICES'',''000050'',''A00''), 
             (''E11'',''OPERATIONS'',''000090'',''E01''), 
             (''E21'',''SOFTWARE SUPPORT'',''000100'',''E01''), 
             (''F22'',''BRANCH OFFICE F2'',NULL,''E01''), 
             (''G22'',''BRANCH OFFICE G2'',NULL,''E01''), 
             (''H22'',''BRANCH OFFICE H2'',NULL,''E01''), 
             (''I22'',''BRANCH OFFICE I2'',NULL,''E01''), 
             (''J22'',''BRANCH OFFICE J2'',NULL,''E01'')');      
           END IF;
      END"""
    
    try:
        stmt = ibm_db.prepare(hdbc,create_department)
        result = ibm_db.execute(stmt)             # Run it
        ibm_db.free_result(stmt)
        
    except Exception as err:
        db2_error(False)
        return

    create_employee = """
     BEGIN
        DECLARE FOUND INTEGER; 
        SET FOUND = (SELECT COUNT(*) FROM SYSIBM.SYSTABLES WHERE NAME='XEMPLOYEE' AND CREATOR=CURRENT USER); 
        IF FOUND = 0 THEN 
          EXECUTE IMMEDIATE('CREATE TABLE XEMPLOYEE(
                             EMPNO CHAR(6) NOT NULL,
                             FIRSTNME VARCHAR(12) NOT NULL,
                             MIDINIT CHAR(1),
                             LASTNAME VARCHAR(15) NOT NULL,
                             WORKDEPT CHAR(3),
                             PHONENO CHAR(4),
                             HIREDATE DATE,
                             JOB CHAR(8),
                             EDLEVEL SMALLINT NOT NULL,
                             SEX CHAR(1),
                             BIRTHDATE DATE,
                             SALARY DECIMAL(9,2),
                             BONUS DECIMAL(9,2),
                             COMM DECIMAL(9,2)
                             )');
          EXECUTE IMMEDIATE('INSERT INTO XEMPLOYEE VALUES
             (''000010'',''CHRISTINE'',''I'',''HAAS''      ,''A00'',''3978'',''1995-01-01'',''PRES    '',18,''F'',''1963-08-24'',152750.00,1000.00,4220.00),
             (''000020'',''MICHAEL''  ,''L'',''THOMPSON''  ,''B01'',''3476'',''2003-10-10'',''MANAGER '',18,''M'',''1978-02-02'',94250.00,800.00,3300.00),
             (''000030'',''SALLY''    ,''A'',''KWAN''      ,''C01'',''4738'',''2005-04-05'',''MANAGER '',20,''F'',''1971-05-11'',98250.00,800.00,3060.00),
             (''000050'',''JOHN''     ,''B'',''GEYER''     ,''E01'',''6789'',''1979-08-17'',''MANAGER '',16,''M'',''1955-09-15'',80175.00,800.00,3214.00),
             (''000060'',''IRVING''   ,''F'',''STERN''     ,''D11'',''6423'',''2003-09-14'',''MANAGER '',16,''M'',''1975-07-07'',72250.00,500.00,2580.00),
             (''000070'',''EVA''      ,''D'',''PULASKI''   ,''D21'',''7831'',''2005-09-30'',''MANAGER '',16,''F'',''2003-05-26'',96170.00,700.00,2893.00),
             (''000090'',''EILEEN''   ,''W'',''HENDERSON'' ,''E11'',''5498'',''2000-08-15'',''MANAGER '',16,''F'',''1971-05-15'',89750.00,600.00,2380.00),
             (''000100'',''THEODORE'' ,''Q'',''SPENSER''   ,''E21'',''0972'',''2000-06-19'',''MANAGER '',14,''M'',''1980-12-18'',86150.00,500.00,2092.00),
             (''000110'',''VINCENZO'' ,''G'',''LUCCHESSI'' ,''A00'',''3490'',''1988-05-16'',''SALESREP'',19,''M'',''1959-11-05'',66500.00,900.00,3720.00),
             (''000120'',''SEAN''     ,'' '',''O`CONNELL'' ,''A00'',''2167'',''1993-12-05'',''CLERK   '',14,''M'',''1972-10-18'',49250.00,600.00,2340.00),
             (''000130'',''DELORES''  ,''M'',''QUINTANA''  ,''C01'',''4578'',''2001-07-28'',''ANALYST '',16,''F'',''1955-09-15'',73800.00,500.00,1904.00),
             (''000140'',''HEATHER''  ,''A'',''NICHOLLS''  ,''C01'',''1793'',''2006-12-15'',''ANALYST '',18,''F'',''1976-01-19'',68420.00,600.00,2274.00),
             (''000150'',''BRUCE''    ,'' '',''ADAMSON''   ,''D11'',''4510'',''2002-02-12'',''DESIGNER'',16,''M'',''1977-05-17'',55280.00,500.00,2022.00),
             (''000160'',''ELIZABETH'',''R'',''PIANKA''    ,''D11'',''3782'',''2006-10-11'',''DESIGNER'',17,''F'',''1980-04-12'',62250.00,400.00,1780.00),
             (''000170'',''MASATOSHI'',''J'',''YOSHIMURA'' ,''D11'',''2890'',''1999-09-15'',''DESIGNER'',16,''M'',''1981-01-05'',44680.00,500.00,1974.00),
             (''000180'',''MARILYN''  ,''S'',''SCOUTTEN''  ,''D11'',''1682'',''2003-07-07'',''DESIGNER'',17,''F'',''1979-02-21'',51340.00,500.00,1707.00),
             (''000190'',''JAMES''    ,''H'',''WALKER''    ,''D11'',''2986'',''2004-07-26'',''DESIGNER'',16,''M'',''1982-06-25'',50450.00,400.00,1636.00),
             (''000200'',''DAVID''    ,'' '',''BROWN''     ,''D11'',''4501'',''2002-03-03'',''DESIGNER'',16,''M'',''1971-05-29'',57740.00,600.00,2217.00),
             (''000210'',''WILLIAM''  ,''T'',''JONES''     ,''D11'',''0942'',''1998-04-11'',''DESIGNER'',17,''M'',''2003-02-23'',68270.00,400.00,1462.00),
             (''000220'',''JENNIFER'' ,''K'',''LUTZ''      ,''D11'',''0672'',''1998-08-29'',''DESIGNER'',18,''F'',''1978-03-19'',49840.00,600.00,2387.00),
             (''000230'',''JAMES''    ,''J'',''JEFFERSON'' ,''D21'',''2094'',''1996-11-21'',''CLERK   '',14,''M'',''1980-05-30'',42180.00,400.00,1774.00),
             (''000240'',''SALVATORE'',''M'',''MARINO''    ,''D21'',''3780'',''2004-12-05'',''CLERK   '',17,''M'',''2002-03-31'',48760.00,600.00,2301.00),
             (''000250'',''DANIEL''   ,''S'',''SMITH''     ,''D21'',''0961'',''1999-10-30'',''CLERK   '',15,''M'',''1969-11-12'',49180.00,400.00,1534.00),
             (''000260'',''SYBIL''    ,''P'',''JOHNSON''   ,''D21'',''8953'',''2005-09-11'',''CLERK   '',16,''F'',''1976-10-05'',47250.00,300.00,1380.00),
             (''000270'',''MARIA''    ,''L'',''PEREZ''     ,''D21'',''9001'',''2006-09-30'',''CLERK   '',15,''F'',''2003-05-26'',37380.00,500.00,2190.00),
             (''000280'',''ETHEL''    ,''R'',''SCHNEIDER'' ,''E11'',''8997'',''1997-03-24'',''OPERATOR'',17,''F'',''1976-03-28'',36250.00,500.00,2100.00),
             (''000290'',''JOHN''     ,''R'',''PARKER''    ,''E11'',''4502'',''2006-05-30'',''OPERATOR'',12,''M'',''1985-07-09'',35340.00,300.00,1227.00),
             (''000300'',''PHILIP''   ,''X'',''SMITH''     ,''E11'',''2095'',''2002-06-19'',''OPERATOR'',14,''M'',''1976-10-27'',37750.00,400.00,1420.00),
             (''000310'',''MAUDE''    ,''F'',''SETRIGHT''  ,''E11'',''3332'',''1994-09-12'',''OPERATOR'',12,''F'',''1961-04-21'',35900.00,300.00,1272.00),
             (''000320'',''RAMLAL''   ,''V'',''MEHTA''     ,''E21'',''9990'',''1995-07-07'',''FIELDREP'',16,''M'',''1962-08-11'',39950.00,400.00,1596.00),
             (''000330'',''WING''     ,'' '',''LEE''       ,''E21'',''2103'',''2006-02-23'',''FIELDREP'',14,''M'',''1971-07-18'',45370.00,500.00,2030.00),
             (''000340'',''JASON''    ,''R'',''GOUNOT''    ,''E21'',''5698'',''1977-05-05'',''FIELDREP'',16,''M'',''1956-05-17'',43840.00,500.00,1907.00),
             (''200010'',''DIAN''     ,''J'',''HEMMINGER'' ,''A00'',''3978'',''1995-01-01'',''SALESREP'',18,''F'',''1973-08-14'',46500.00,1000.00,4220.00),
             (''200120'',''GREG''     ,'' '',''ORLANDO''   ,''A00'',''2167'',''2002-05-05'',''CLERK   '',14,''M'',''1972-10-18'',39250.00,600.00,2340.00),
             (''200140'',''KIM''      ,''N'',''NATZ''      ,''C01'',''1793'',''2006-12-15'',''ANALYST '',18,''F'',''1976-01-19'',68420.00,600.00,2274.00),
             (''200170'',''KIYOSHI''  ,'' '',''YAMAMOTO''  ,''D11'',''2890'',''2005-09-15'',''DESIGNER'',16,''M'',''1981-01-05'',64680.00,500.00,1974.00),
             (''200220'',''REBA''     ,''K'',''JOHN''      ,''D11'',''0672'',''2005-08-29'',''DESIGNER'',18,''F'',''1978-03-19'',69840.00,600.00,2387.00),
             (''200240'',''ROBERT''   ,''M'',''MONTEVERDE'',''D21'',''3780'',''2004-12-05'',''CLERK   '',17,''M'',''1984-03-31'',37760.00,600.00,2301.00),
             (''200280'',''EILEEN''   ,''R'',''SCHWARTZ''  ,''E11'',''8997'',''1997-03-24'',''OPERATOR'',17,''F'',''1966-03-28'',46250.00,500.00,2100.00),
             (''200310'',''MICHELLE'' ,''F'',''SPRINGER''  ,''E11'',''3332'',''1994-09-12'',''OPERATOR'',12,''F'',''1961-04-21'',35900.00,300.00,1272.00),
             (''200330'',''HELENA''   ,'' '',''WONG''      ,''E21'',''2103'',''2006-02-23'',''FIELDREP'',14,''F'',''1971-07-18'',35370.00,500.00,2030.00),
             (''200340'',''ROY''      ,''R'',''ALONZO''    ,''E21'',''5698'',''1997-07-05'',''FIELDREP'',16,''M'',''1956-05-17'',31840.00,500.00,1907.00)');                             
        END IF;
     END"""
     
    try: 
        stmt = ibm_db.prepare(hdbc,create_employee)
        result = ibm_db.execute(stmt)             # Run it
        ibm_db.free_result(stmt)

    except Exception as err:
        db2_error(False)
        return
    
    if (quiet == False): success("Sample tables [EMPLOYEE, DEPARTMENT] created.")
    
def checkOption(args_in, option, vFalse=False, vTrue=True):
    
    args_out = args_in.strip()
    found = vFalse
    
    if (args_out != ""):
        if (args_out.find(option) >= 0):
            args_out = args_out.replace(option," ")
            args_out = args_out.strip()
            found = vTrue

    return args_out, found
    
def plotData(flag_plot, hdbi, sql, parms):
       
    try:
        if (len(parms) != 0):                             # There are parameters we need to take care of
            df = pandas.read_sql(sql,hdbi,params=parms)
        else:
            df = pandas.read_sql(sql,hdbi)
          
    except Exception as err:
        db2_error(False)
        return
                
    if flag_plot == 4:                                    # Plot 4 = pixiedust
        ShellAccess.pdf = df
        _ = display(pdf)
        return

    col_count = len(df.columns)
                
    if flag_plot == 1:                                    # Plot 1 = bar chart

        if (col_count in (1,2,3)):
            plt.style.use('ggplot');
            plt.figure();   
            
            if (col_count == 1):
                df.index = df.index + 1
                _ = df.plot(kind='bar');
            elif (col_count == 2):
                xlabel = df.columns.values[0]
                ylabel = df.columns.values[1]
                _ = df.plot(kind='bar',x=xlabel,y=ylabel);
            else:
                values = df.columns.values[2]
                columns = df.columns.values[0]
                index = df.columns.values[1]
                pivoted = pandas.pivot_table(df, values=values, columns=columns, index=index) 
                _ = pivoted.plot.bar(); 
                
            plt.show();
            
        else:
            ShellAccess.pdf = df
            _ = pDisplay(pdf);
                    
    elif flag_plot == 2:                                  # Plot 2 = pie chart
        
        if (col_count in (1,2)):
            plt.style.use('ggplot');
            plt.figure();   
                
            if (col_count == 1):
                df.index = df.index + 1
                yname = df.columns.values[0]
                _ = df.plot(kind='pie',y=yname);                
            else:          
                xlabel = df.columns.values[0]
                xname = df[xlabel].tolist()
                yname = df.columns.values[1]
                _ = df.plot(kind='pie',y=yname,labels=xname);
                
            plt.show();
            
        else:
            ShellAccess.pdf = df
            _ = pDisplay(pdf);
                    
    elif flag_plot == 3:                                  # Plot 3 = line chart
            
        if (col_count in (1,2,3)):
            plt.style.use('ggplot');
            plt.figure();   
            
            if (col_count == 1):
                df.index = df.index + 1  
                _ = df.plot(kind='line');          
            elif (col_count == 2):            
                xlabel = df.columns.values[0]
                ylabel = df.columns.values[1]
                _ = df.plot(kind='line',x=xlabel,y=ylabel) ; 
            else:         
                values = df.columns.values[2]
                columns = df.columns.values[0]
                index = df.columns.values[1]
                pivoted = pandas.pivot_table(df, values=values, columns=columns, index=index)
                _ = pivoted.plot();
                
            plt.show();
                
        else:
            ShellAccess.pdf = df
            _ = pDisplay(pdf);
    else:
        return   
        
@magics_class
class Db2(Magics):
      
    import pixiedust  
    
    @line_cell_magic
    def sql(self, line, cell=None):
            
        # Before we event get started, check to see if you have connected yet. Without a connection we 
        # can't do anything. You may have a connection request in the code, so if that is true, we run those,
        # otherwise we connect immediately
        
        # If your statement is not a connect, and you haven't connected, we need to do it for you
    
        global settings 
        global hdbc, hdbi, connected
           
        # If you use %sql (line) we just run the SQL. If you use %%SQL the entire cell is run.
        
        flag_delim = ";"
        flag_results = True
        flag_quiet = False
        flag_json = False
        flag_timer = False
        flag_plot = 0
        flag_cell = False
        flag_output = False
        flag_raw = False
        flag_dataframe = False
        flag_variables = False 
        
        # The parameters must be in the line, not in the cell i.e. %sql -c 
        
        allSQL = line.strip()
        
        if len(allSQL) == 0:
            if cell == None: 
                sqlhelp()
                return
            if len(cell.strip()) == 0: 
                sqlhelp()
                return
            
        if allSQL == "?" or allSQL == "-h":                             # Are you asking for help
            sqlhelp()
            return
        
        if allSQL.upper() == "? CONNECT":                              # Are you asking for help on CONNECT
            connected_help()
            return
        
        sqltype, _, _ = sqlParser(allSQL)                              # If this is a CONNECT command, run it alone
        if (sqltype == "CONNECT"):
            parseConnect(allSQL)
            return
        
        if (connected == False):                                      # Check if you are connected 
            db2_doConnect()
            if connected == False: return
            
        if settings["maxrows"] == -1:                                 # Set the return result size
            pandas.reset_option('max_rows')
        else:
            pandas.options.display.max_rows = settings["maxrows"]
      
    
        allSQL, flag_json  = checkOption(allSQL,'-j')                 # Look for JSON formatting
        allSQL, flag_raw   = checkOption(allSQL,'-r')                 # Place results into a 2-dim array
        allSQL, flag_quiet = checkOption(allSQL,'-q')                 # No error messages produced
        allSQL, flag_delim = checkOption(allSQL,'-d',';','@')         # Change the delimiter    
        allSQL, flag_timer = checkOption(allSQL,'-t')                 # Timer flag  
        allSQL, flag_plot  = checkOption(allSQL,'-pb',flag_plot,1)    # Bar chart
        allSQL, flag_plot  = checkOption(allSQL,'-pp',flag_plot,2)    # Pie chart
        allSQL, flag_plot  = checkOption(allSQL,'-pl',flag_plot,3)    # Line chart
        allSQL, flag_plot  = checkOption(allSQL,'-i', flag_plot,4)    # Pixiedust chart        
        
        allSQL, flag       = checkOption(allSQL,'-a')                 # Change maximum number of rows returned
        if (flag == True):
            pandas.options.display.max_rows = None #reset_option('max_rows')

        allSQL, flag       = checkOption(allSQL,'-sampledata')        # Create SAMPLE tables
        if (flag == True):
            db2_create_sample(flag_quiet)
            return
        
        remainder = allSQL
                               
        if cell is None:                                              # Split the input (only if cell mode)
            sqlLines = [remainder]
            flag_cell = False
        else:
            cell = re.sub('.*?--.*$',"",cell,flags=re.M)
            remainder = cell.replace("\n"," ")
            sqlLines = remainder.split(flag_delim)
            flag_cell = True
                      
        # For each line figure out if you run it as a command (db2) or select (sql)
         
        for sqlin in sqlLines:                                        # Run each command
            
            sqlType, parms, sql = sqlParser(sqlin)                    # Parse the SQL
            
            if (sql == ""): continue
 
            if (flag_timer == True):
                cnt = sqlTimer(hdbc, runtime, sql, parms)             # Given the sql and parameters, clock the time
                if (cnt >= 0): print("Total iterations in %s second(s): %s" % (runtime,cnt))                
                return cnt
            
            elif (flag_plot != 0):                                    # We are plotting some results 
                
                plotData(flag_plot,hdbi, sql,parms)                   # Plot the data and return
 
            else:
                try:                                                  # See if we have an answer set
                    stmt = ibm_db.prepare(hdbc,sql)
                    if (ibm_db.num_fields(stmt) == 0):                # No, so we just execute the code
                        if (len(parms) == 0):                         # Check if parameters present
                            result = ibm_db.execute(stmt)             # Run it
                        else:                                         # Include parameters
                            result = ibm_db.execute(stmt,tuple(parms))# Run the code and check it
                        ibm_db.free_result(stmt)
                        if (result == False):                         # Error executing the code
                            db2_error(flag_quiet)
                            return
                    
                    elif (flag_raw == True or flag_json == True):           # Prefer the output as an array
                        row_count = 0
                        resultSet = []
                        try:
                            if (len(parms) == 0):                         # Check if parameters present
                                result = ibm_db.execute(stmt)             # Run it
                            else:                                         # Include parameters
                                result = ibm_db.execute(stmt,tuple(parms))# Run the code and check it
                            if (result == False):                         # Error executing the code
                                db2_error(flag_quiet)  
                                ibm_db.free_result(stmt)
                                return
                                
                            if (flag_json == True):                        # JSON output
                                row_count = 0
                                while( ibm_db.fetch_row(stmt) ):
                                    row_count = row_count + 1
                                    jsonVal = ibm_db.result(stmt,0)
                                    formatted_JSON = json.dumps(json.loads(jsonVal), indent=4, separators=(',', ': '))
                                    if row_count > 1: print()
                                    print("Row: %d" % row_count)
                                    print(formatted_JSON)
                                    flag_output = True                                    
                                
                                ibm_db.free_result(stmt)
                                return
                                
                            else:
                                result = ibm_db.fetch_tuple(stmt)
                                while (result):
                                    row = []
                                    for col in result:
                                        row.append(col)
                                    resultSet.append(row)
                                    result = ibm_db.fetch_tuple(stmt)
                            
                                ibm_db.free_result(stmt)
                                return(resultSet)                                    
                                
                        except Exception as err:
                            db2_error(flag_quiet)
                            return
                    else:
                        ibm_db.free_result(stmt)
                        if (len(parms) > 0): 
                            dp = pandas.read_sql(sql, hdbi ,params=parms)
                        else:
                            dp = pandas.read_sql(sql, hdbi)
                        flag_output = True
                        return(dp)
 

                except:
                    db2_error(flag_quiet)
                    return
                

        if (flag_output == False and flag_quiet == False): print("Command completed.")                     