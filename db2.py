#
# Set up Jupyter MAGIC commands "sql". 
# %sql will return results from a DB2 select statement or execute a DB2 command
#
# IBM 2019: George Baklarz
# Version 2019-10-03
#

from __future__ import print_function
from IPython.display import HTML as pHTML, Image as pImage, display as pdisplay, Javascript as Javascript
from IPython.core.magic import (Magics, magics_class, line_magic,
                                cell_magic, line_cell_magic, needs_local_scope)
import ibm_db
import pandas
import ibm_db_dbi
import json
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

_settings = {
     "maxrows"  : 10,
     "maxgrid"  : 5,
     "runtime"  : 1,
     "display"  : "PANDAS",
     "database" : "",
     "hostname" : "localhost",
     "port"     : "50000",
     "protocol" : "TCPIP",    
     "uid"      : "DB2INST1",
     "pwd"      : "password",
     "ssl"      : ""
}

_environment = {
     "jupyter"  : True,
     "qgrid"    : True
}

_display = {
    'fullWidthRows': True,
    'syncColumnCellResize': True,
    'forceFitColumns': False,
    'defaultColumnWidth': 150,
    'rowHeight': 28,
    'enableColumnReorder': False,
    'enableTextSelectionOnCells': True,
    'editable': False,
    'autoEdit': False,
    'explicitInitialization': True,
    'maxVisibleRows': 5,
    'minVisibleRows': 5,
    'sortable': True,
    'filterable': False,
    'highlightSelectedCell': False,
    'highlightSelectedRow': True
}

# Connection settings for statements 

_connected = False
_hdbc = None
_hdbi = None
_stmt = []
_stmtID = []
_stmtSQL = []
_vars = {}
_macros = {}
_flags = []
_debug = False

# Db2 Error Messages and Codes
sqlcode = 0
sqlstate = "0"
sqlerror = ""
sqlelapsed = 0

# Check to see if QGrid is installed

try:
    import qgrid
    qgrid.set_defaults(grid_options=_display)
except:
    _environment['qgrid'] = False
    
# Check if we are running in iPython or Jupyter

try:
    if (get_ipython().config == {}): 
        _environment['jupyter'] = False
        _environment['qgrid'] = False
    else:
        _environment['jupyter'] = True
except:
    _environment['jupyter'] = False
    _environment['qgrid'] = False


def setOptions(inSQL):

    global _settings, _display

    cParms = inSQL.split()
    cnt = 0

    while cnt < len(cParms):
        if cParms[cnt].upper() == 'MAXROWS':
            
            if cnt+1 < len(cParms):
                try:
                    _settings["maxrows"] = int(cParms[cnt+1])
                except Exception as err:
                    errormsg("Invalid MAXROWS value provided.")
                    pass
                cnt = cnt + 1
            else:
                errormsg("No maximum rows specified for the MAXROWS option.")
                return
            
        elif cParms[cnt].upper() == 'MAXGRID':
            
            if cnt+1 < len(cParms):
                try:
                    maxgrid = int(cParms[cnt+1])
                    if (maxgrid <= 5):                      # Minimum window size is 5
                        maxgrid = 5
                    _display["maxVisibleRows"] =  int(cParms[cnt+1])
                    try:
                        import qgrid
                        qgrid.set_defaults(grid_options=_display)
                    except:
                        _environment['qgrid'] = False
                        
                except Exception as err:
                    errormsg("Invalid MAXGRID value provided.")
                    pass
                cnt = cnt + 1
            else:
                errormsg("No maximum rows specified for the MAXROWS option.")
                return            
            
        elif cParms[cnt].upper() == 'RUNTIME':
            if cnt+1 < len(cParms):
                try:
                    _settings["runtime"] = int(cParms[cnt+1])
                except Exception as err:
                    errormsg("Invalid RUNTIME value provided.")
                    pass
                cnt = cnt + 1
            else:
                errormsg("No value provided for the RUNTIME option.")
                return 
            
        elif cParms[cnt].upper() == 'DISPLAY':
            if cnt+1 < len(cParms):
                if (cParms[cnt+1].upper() == 'GRID'):
                    _settings["display"] = 'GRID'
                elif (cParms[cnt+1].upper()  == 'PANDAS'):
                    _settings["display"] = 'PANDAS'
                else:
                    errormsg("Invalid DISPLAY value provided.")
                cnt = cnt + 1
            else:
                errormsg("No value provided for the DISPLAY option.")
                return  
        elif (cParms[cnt].upper() == 'LIST'):
            print("(MAXROWS) Maximum number of rows displayed: " + str(_settings["maxrows"]))
            print("(MAXGRID) Maximum grid display size: " + str(_settings["maxgrid"]))
            print("(RUNTIME) How many seconds to a run a statement for performance testing: " + str(_settings["runtime"]))
            print("(DISPLAY) Use PANDAS or GRID display format for output: " + _settings["display"]) 
            return
        else:
            cnt = cnt + 1
            
    save_settings()

def sqlhelp():
    
    global _environment
    
    if (_environment["jupyter"] == True):
        sd  = '<td style="text-align:left;">'
        ed1 = '</td>'
        ed2 = '</td>'
        sh  = '<th style="text-align:left;">'
        eh1 = '</th>'
        eh2 = '</th>'
        sr  = '<tr>'
        er  = '</tr>'
        helpSQL = """
        <h3>SQL Options</h3> 
        <p>The following options are available as part of a SQL statement. The options are always preceded with a
        minus sign (i.e. -q).
        <table>
         {sr}
            {sh}Option{eh1}{sh}Description{eh2}
         {er}
         {sr}
            {sd}a, all{ed1}{sd}Return all rows in answer set and do not limit display{ed2}
         {er}       
         {sr}
           {sd}d{ed1}{sd}Change SQL delimiter to "@" from ";"{ed2}
         {er}
         {sr}
           {sd}e, echo{ed1}{sd}Echo the SQL command that was generated after macro and variable substituion.{ed2}
         {er}
         {sr}
           {sd}h, help{ed1}{sd}Display %sql help information.{ed2}
         {er}        
         {sr}
           {sd}j{ed1}{sd}Create a pretty JSON representation. Only the first column is formatted{ed2}
         {er}
         {sr}
           {sd}json{ed1}{sd}Retrieve the result set as a JSON record{ed2}
         {er} 
         {sr}
           {sd}q, quiet{ed1}{sd}Quiet results - no answer set or messages returned from the function{ed2}
         {er}
         {sr}  
           {sd}r, array{ed1}{sd}Return the result set as an array of values{ed2}
         {er}
         {sr}
           {sd}sampledata{ed1}{sd}Create and load the EMPLOYEE and DEPARTMENT tables{ed2}
         {er}        
         {sr}
           {sd}t,time{ed1}{sd}Time the following SQL statement and return the number of times it executes in 1 second{ed2}
         {er}
         {sr}
           {sd}grid{ed1}{sd}Display the results in a scrollable grid{ed2}
         {er}       
        
        </table>
       """        
    else:
        helpSQL = """
SQL Options

The following options are available as part of a SQL statement. Options are always 
preceded with a minus sign (i.e. -q).

Option     Description
a, all     Return all rows in answer set and do not limit display 
d          Change SQL delimiter to "@" from ";" 
e, echo    Echo the SQL command that was generated after substitution 
h, help    Display %sql help information
j          Create a pretty JSON representation. Only the first column is formatted 
json       Retrieve the result set as a JSON record 
q, quiet   Quiet results - no answer set or messages returned from the function 
r, array   Return the result set as an array of values 
t,time     Time the SQL statement and return the execution count per second
grid       Display the results in a scrollable grid 
       """        
    helpSQL = helpSQL.format(**locals())
    
    if (_environment["jupyter"] == True):
        pdisplay(pHTML(helpSQL))
    else:
        print(helpSQL)

def connected_help():
    
   
    sd = '<td style="text-align:left;">'
    ed = '</td>'
    sh = '<th style="text-align:left;">'
    eh = '</th>'
    sr = '<tr>'
    er = '</tr>'
    
    if (_environment['jupyter'] == True):
        
        helpConnect = """
       <h3>Connecting to Db2</h3> 
       <p>The CONNECT command has the following format:
       <p>
       <pre>
       %sql CONNECT TO &lt;database&gt; USER &lt;userid&gt; USING &lt;password|?&gt; HOST &lt;ip address&gt; PORT &lt;port number&gt; &lt;SSL&gt;
       %sql CONNECT CREDENTIALS &lt;varname&gt;
       %sql CONNECT CLOSE
       %sql CONNECT RESET
       %sql CONNECT PROMPT - use this to be prompted for values
       </pre>
       <p>
       If you use a "?" for the password field, the system will prompt you for a password. This avoids typing the 
       password as clear text on the screen. If a connection is not successful, the system will print the error
       message associated with the connect request.
       <p>
       The <b>CREDENTIALS</b> option allows you to use credentials that are supplied by Db2 on Cloud instances.
       The credentials can be supplied as a variable and if successful, the variable will be saved to disk 
       for future use. If you create another notebook and use the identical syntax, if the variable 
       is not defined, the contents on disk will be used as the credentials. You should assign the 
       credentials to a variable that represents the database (or schema) that you are communicating with. 
       Using familiar names makes it easier to remember the credentials when connecting. 
       <p>
       <b>CONNECT CLOSE</b> will close the current connection, but will not reset the database parameters. This means that
       if you issue the CONNECT command again, the system should be able to reconnect you to the database.
       <p>
       <b>CONNECT RESET</b> will close the current connection and remove any information on the connection. You will need 
       to issue a new CONNECT statement with all of the connection information.
       <p>
       If the connection is successful, the parameters are saved on your system and will be used the next time you
       run an SQL statement, or when you issue the %sql CONNECT command with no parameters.
       <p>If you issue CONNECT RESET, all of the current values will be deleted and you will need to 
       issue a new CONNECT statement. 
       <p>A CONNECT command without any parameters will attempt to re-connect to the previous database you 
       were using. If the connection could not be established, the program to prompt you for
       the values. To cancel the connection attempt, enter a blank value for any of the values. The connection 
       panel will request the following values in order to connect to Db2: 
       <table>
       {sr}
         {sh}Setting{eh}
         {sh}Description{eh}
       {er}
       {sr}
         {sd}Database{ed}{sd}Database name you want to connect to.{ed}
       {er}
       {sr}
         {sd}Hostname{ed}
         {sd}Use localhost if Db2 is running on your own machine, but this can be an IP address or host name. 
       {er}
       {sr}
         {sd}PORT{ed}
         {sd}The port to use for connecting to Db2. This is usually 50000.{ed}
       {er}
       {sr}
         {sd}SSL{ed}
         {sd}If you are connecting to a secure port (50001) with SSL then you must include this keyword in the connect string.{ed}
       {sr}         
         {sd}Userid{ed}
         {sd}The userid to use when connecting (usually DB2INST1){ed} 
       {er}
       {sr}                  
         {sd}Password{ed}
         {sd}No password is provided so you have to enter a value{ed}
       {er}
        </table>
       """
    else:
        helpConnect = """\
Connecting to Db2

The CONNECT command has the following format:

%sql CONNECT TO database USER userid USING password | ? 
                HOST ip address PORT port number SSL
%sql CONNECT CREDENTIALS varname
%sql CONNECT CLOSE
%sql CONNECT RESET

If you use a "?" for the password field, the system will prompt you for a password.
This avoids typing the password as clear text on the screen. If a connection is 
not successful, the system will print the error message associated with the connect
request.

The CREDENTIALS option allows you to use credentials that are supplied by Db2 on 
Cloud instances. The credentials can be supplied as a variable and if successful, 
the variable will be saved to disk for future use. If you create another notebook
and use the identical syntax, if the variable is not defined, the contents on disk
will be used as the credentials. You should assign the credentials to a variable 
that represents the database (or schema) that you are communicating with. Using 
familiar names makes it easier to remember the credentials when connecting. 

CONNECT CLOSE will close the current connection, but will not reset the database 
parameters. This means that if you issue the CONNECT command again, the system 
should be able to reconnect you to the database.

CONNECT RESET will close the current connection and remove any information on the
connection. You will need to issue a new CONNECT statement with all of the connection
information.

If the connection is successful, the parameters are saved on your system and will be
used the next time you run an SQL statement, or when you issue the %sql CONNECT 
command with no parameters. If you issue CONNECT RESET, all of the current values 
will be deleted and you will need to issue a new CONNECT statement. 

A CONNECT command without any parameters will attempt to re-connect to the previous 
database you were using. If the connection could not be established, the program to
prompt you for the values. To cancel the connection attempt, enter a blank value for
any of the values. The connection panel will request the following values in order 
to connect to Db2: 

  Setting    Description
  Database   Database name you want to connect to
  Hostname   Use localhost if Db2 is running on your own machine, but this can 
             be an IP address or host name. 
  PORT       The port to use for connecting to Db2. This is usually 50000. 
  Userid     The userid to use when connecting (usually DB2INST1) 
  Password   No password is provided so you have to enter a value
  SSL        Include this keyword to indicate you are connecting via SSL (usually port 50001)
"""
    
    helpConnect = helpConnect.format(**locals())
    
    if (_environment['jupyter'] == True):
        pdisplay(pHTML(helpConnect))
    else:
        print(helpConnect)

# Prompt for Connection information

def connected_prompt():
    
    global _settings
    
    _database = ''
    _hostname = ''
    _port = ''
    _uid = ''
    _pwd = ''
    _ssl = ''
    
    print("Enter the database connection details (Any empty value will cancel the connection)")
    _database = input("Enter the database name: ");
    if (_database.strip() == ""): return False
    _hostname = input("Enter the HOST IP address or symbolic name: ");
    if (_hostname.strip() == ""): return False 
    _port = input("Enter the PORT number: ");
    if (_port.strip() == ""): return False    
    _ssl = input("Is this a secure (SSL) port (y or n)");
    if (_ssl.strip() == ""): return False
    if (_ssl == "n"):
        _ssl = ""
    else:
        _ssl = "Security=SSL;" 
    _uid = input("Enter Userid on the DB2 system: ").upper();
    if (_uid.strip() == ""): return False
    _pwd = getpass.getpass("Password [password]: ");
    if (_pwd.strip() == ""): return False
        
    _settings["database"] = _database.strip()
    _settings["hostname"] = _hostname.strip()
    _settings["port"] = _port.strip()
    _settings["uid"] = _uid.strip()
    _settings["pwd"] = _pwd.strip()
    _settings["ssl"] = _ssl.strip()
    _settings["maxrows"] = 10
    _settings["maxgrid"] = 5
    _settings["runtime"] = 1
    
    return True
    
# Split port and IP addresses

def split_string(in_port,splitter=":"):
 
    # Split input into an IP address and Port number
    
    global _settings

    checkports = in_port.split(splitter)
    ip = checkports[0]
    if (len(checkports) > 1):
        port = checkports[1]
    else:
        port = None

    return ip, port

# Parse the CONNECT statement and execute if possible 

def parseConnect(inSQL,local_ns):
    
    global _settings, _connected

    _connected = False
    
    cParms = inSQL.split()
    cnt = 0
    
    _settings["ssl"] = ""
    
    while cnt < len(cParms):
        if cParms[cnt].upper() == 'TO':
            if cnt+1 < len(cParms):
                _settings["database"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No database specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == "SSL":
            _settings["ssl"] = "Security=SSL;"  
            cnt = cnt + 1
        elif cParms[cnt].upper() == 'CREDENTIALS':
            if cnt+1 < len(cParms):
                credentials = cParms[cnt+1]
                tempid = eval(credentials,local_ns)
                if (isinstance(tempid,dict) == False): 
                    errormsg("The CREDENTIALS variable (" + credentials + ") does not contain a valid Python dictionary (JSON object)")
                    return
                if (tempid == None):
                    fname = credentials + ".pickle"
                    try:
                        with open(fname,'rb') as f: 
                            _id = pickle.load(f) 
                    except:
                        errormsg("Unable to find credential variable or file.")
                        return
                else:
                    _id = tempid
                    
                try:
                    _settings["database"] = _id["db"]
                    _settings["hostname"] = _id["hostname"]
                    _settings["port"] = _id["port"]
                    _settings["uid"] = _id["username"]
                    _settings["pwd"] = _id["password"]
                    try:
                        fname = credentials + ".pickle"
                        with open(fname,'wb') as f:
                            pickle.dump(_id,f)
            
                    except:
                        errormsg("Failed trying to write Db2 Credentials.")
                        return
                except:
                    errormsg("Credentials file is missing information. db/hostname/port/username/password required.")
                    return
                     
            else:
                errormsg("No Credentials name supplied")
                return
            
            cnt = cnt + 1
              
        elif cParms[cnt].upper() == 'USER':
            if cnt+1 < len(cParms):
                _settings["uid"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No userid specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'USING':
            if cnt+1 < len(cParms):
                _settings["pwd"] = cParms[cnt+1]   
                if (_settings["pwd"] == '?'):
                    _settings["pwd"] = getpass.getpass("Password [password]: ") or "password"
                cnt = cnt + 1
            else:
                errormsg("No password specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'HOST':
            if cnt+1 < len(cParms):
                hostport = cParms[cnt+1].upper()
                ip, port = split_string(hostport)
                if (port == None): _settings["port"] = "50000"
                _settings["hostname"] = ip
                cnt = cnt + 1
            else:
                errormsg("No hostname specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'PORT':                           
            if cnt+1 < len(cParms):
                _settings["port"] = cParms[cnt+1].upper()
                cnt = cnt + 1
            else:
                errormsg("No port specified in the CONNECT statement")
                return
        elif cParms[cnt].upper() == 'PROMPT':
            if (connected_prompt() == False): 
                print("Connection canceled.")
                return 
            else:
                cnt = cnt + 1
        elif cParms[cnt].upper() in ('CLOSE','RESET') :
            try:
                result = ibm_db.close(_hdbc)
                _hdbi.close()
            except:
                pass
            success("Connection closed.")          
            if cParms[cnt].upper() == 'RESET': 
                _settings["database"] = ''
            return
        else:
            cnt = cnt + 1
                     
    _ = db2_doConnect()

def db2_doConnect():
    
    global _hdbc, _hdbi, _connected, _runtime
    global _settings  

    if _connected == False: 
        
        if len(_settings["database"]) == 0:
            return False

    dsn = (
           "DRIVER={{IBM DB2 ODBC DRIVER}};"
           "DATABASE={0};"
           "HOSTNAME={1};"
           "PORT={2};"
           "PROTOCOL=TCPIP;"
           "UID={3};"
           "PWD={4};{5}").format(_settings["database"], 
                                 _settings["hostname"], 
                                 _settings["port"], 
                                 _settings["uid"], 
                                 _settings["pwd"],
                                 _settings["ssl"])

    # Get a database handle (hdbc) and a statement handle (hstmt) for subsequent access to DB2

    try:
        _hdbc  = ibm_db.connect(dsn, "", "")
    except Exception as err:
        db2_error(False,True) # errormsg(str(err))
        _connected = False
        _settings["database"] = ''
        return False
    
    try:
        _hdbi = ibm_db_dbi.Connection(_hdbc)
    except Exception as err:
        db2_error(False,True) # errormsg(str(err))
        _connected = False
        _settings["database"] = ''
        return False  
    
    _connected = True
    
    # Save the values for future use
    
    save_settings()
    
    success("Connection successful.")
    return True
    

def load_settings():

    # This routine will load the settings from the previous session if they exist
    
    global _settings
    
    fname = "db2connect.pickle"

    try:
        with open(fname,'rb') as f: 
            _settings = pickle.load(f) 
                
        # Reset runtime to 1 since it would be unexpected to keep the same value between connections         
        _settings["runtime"] = 1
        _settings["maxgrid"] = 5
        
    except: 
        pass
    
    return
    
def save_settings():

    # This routine will save the current settings if they exist
    
    global _settings
    
    fname = "db2connect.pickle"
    
    try:
        with open(fname,'wb') as f:
            pickle.dump(_settings,f)
            
    except:
        errormsg("Failed trying to write Db2 Configuration Information.")
 
    return  
    
    try:
        with open(fname,'wb') as f:
            pickle.dump(_settings,f)
            
    except:
        errormsg("Failed trying to write Db2 Configuration Information.")
 
    return  

def db2_error(quiet,connect=False):
    
    global sqlerror, sqlcode, sqlstate, _environment
    
    
    try:
        if (connect == False):
            errmsg = ibm_db.stmt_errormsg().replace('\r',' ')
            errmsg = errmsg[errmsg.rfind("]")+1:].strip()
        else:
            errmsg = ibm_db.conn_errormsg().replace('\r',' ')
            errmsg = errmsg[errmsg.rfind("]")+1:].strip()
            
        sqlerror = errmsg
 
        msg_start = errmsg.find("SQLSTATE=")
        if (msg_start != -1):
            msg_end = errmsg.find(" ",msg_start)
            if (msg_end == -1):
                msg_end = len(errmsg)
            sqlstate = errmsg[msg_start+9:msg_end]
        else:
            sqlstate = "0"
    
        msg_start = errmsg.find("SQLCODE=")
        if (msg_start != -1):
            msg_end = errmsg.find(" ",msg_start)
            if (msg_end == -1):
                msg_end = len(errmsg)
            sqlcode = errmsg[msg_start+8:msg_end]
            try:
                sqlcode = int(sqlcode)
            except:
                pass
        else:        
            sqlcode = 0
            
    except:
        errmsg = "Unknown error."
        sqlcode = -99999
        sqlstate = "-99999"
        sqlerror = errmsg
        return
        
    
    msg_start = errmsg.find("SQLSTATE=")
    if (msg_start != -1):
        msg_end = errmsg.find(" ",msg_start)
        if (msg_end == -1):
            msg_end = len(errmsg)
        sqlstate = errmsg[msg_start+9:msg_end]
    else:
        sqlstate = "0"
        
    
    msg_start = errmsg.find("SQLCODE=")
    if (msg_start != -1):
        msg_end = errmsg.find(" ",msg_start)
        if (msg_end == -1):
            msg_end = len(errmsg)
        sqlcode = errmsg[msg_start+8:msg_end]
        try:
            sqlcode = int(sqlcode)
        except:
            pass
    else:
        sqlcode = 0
    
    if quiet == True: return
    
    if (errmsg == ""): return

    html = '<p><p style="border:2px; border-style:solid; border-color:#FF0000; background-color:#ffe6e6; padding: 1em;">'
    
    if (_environment["jupyter"] == True):
        pdisplay(pHTML(html+errmsg+"</p>"))
    else:
        print(errmsg)
    
# Print out an error message

def errormsg(message):
    
    global _environment
    
    if (message != ""):
        html = '<p><p style="border:2px; border-style:solid; border-color:#FF0000; background-color:#ffe6e6; padding: 1em;">'
        if (_environment["jupyter"] == True):
            pdisplay(pHTML(html + message + "</p>"))     
        else:
            print(message)
    
def success(message):
    
    if (message != ""):
        print(message)
    return   

def debug(message,error=False):
    
    global _environment
    
    if (_environment["jupyter"] == True):
        spacer = "<br>" + "&nbsp;"
    else:
        spacer = "\n "
    
    if (message != ""):

        lines = message.split('\n')
        msg = ""
        indent = 0
        for line in lines:
            delta = line.count("(") - line.count(")")
            if (msg == ""):
                msg = line
                indent = indent + delta
            else:
                if (delta < 0): indent = indent + delta
                msg = msg + spacer * (indent*2) + line
                if (delta > 0): indent = indent + delta    

            if (indent < 0): indent = 0
        if (error == True):        
            html = '<p><pre style="font-family: monospace; border:2px; border-style:solid; border-color:#FF0000; background-color:#ffe6e6; padding: 1em;">'                  
        else:
            html = '<p><pre style="font-family: monospace; border:2px; border-style:solid; border-color:#008000; background-color:#e6ffe6; padding: 1em;">'
        
        if (_environment["jupyter"] == True):
            pdisplay(pHTML(html + msg + "</pre></p>"))
        else:
            print(msg)
        
    return 

def setMacro(inSQL,parms):
      
    global _macros
    
    names = parms.split()
    if (len(names) < 2):
        errormsg("No command name supplied.")
        return None
    
    macroName = names[1].upper()
    _macros[macroName] = inSQL

    return

def checkMacro(in_sql):
       
    global _macros
    
    if (len(in_sql) == 0): return(in_sql)          # Nothing to do 
    
    tokens = parseArgs(in_sql,None)                # Take the string and reduce into tokens
    
    macro_name = tokens[0].upper()                 # Uppercase the name of the token
 
    if (macro_name not in _macros): 
        return(in_sql) # No macro by this name so just return the string

    result = runMacro(_macros[macro_name],in_sql,tokens)  # Execute the macro using the tokens we found

    return(result)                                 # Runmacro will either return the original SQL or the new one

def parseCallArgs(macro):
    
    quoteChar = ""
    inQuote = False
    inParm = False
    name = ""
    parms = []
    parm = ''
    
    sql = macro
    
    for ch in macro:
        if (inParm == False):
            if (ch in ["("," ","\n"]): 
                inParm = True
            else:
                name = name + ch
        else:
            if (inQuote == True):
                if (ch == quoteChar):
                    inQuote = False  
                    #if (quoteChar == "]"):
                    #    parm = parm + "'"
                else:
                    parm = parm + ch
            elif (ch in ("\"","\'","[")): # Do we have a quote
                if (ch == "["):
                   # parm = parm + "'"
                    quoteChar = "]"
                else:
                    quoteChar = ch
                inQuote = True
            elif (ch == ")"):
                if (parm != ""):
                    parm_name, parm_value = splitassign(parm)
                    parms.append([parm_name,parm_value])
                parm = ""
                break
            elif (ch == ","):
                if (parm != ""):
                    parm_name, parm_value = splitassign(parm)
                    parms.append([parm_name,parm_value])                  
                else:
                    parms.append(["null","null"])
                    
                parm = ""

            else:
                parm = parm + ch
                
    if (inParm == True):
        if (parm != ""):
            parm_name, parm_value = splitassign(parm)
            parms.append([parm_name,parm_value])      
       
    return(name,parms)

def splitassign(arg):
    
    var_name = "null"
    var_value = "null"
    
    arg = arg.strip()
    eq = arg.find("=")
    if (eq != -1):
        var_name = arg[:eq].strip()
        temp_value = arg[eq+1:].strip()
        if (temp_value != ""):
            ch = temp_value[0]
            if (ch in ["'",'"']):
                if (temp_value[-1:] == ch):
                    var_value = temp_value[1:-1]
                else:
                    var_value = temp_value
            else:
                var_value = temp_value
    else:
        var_value = arg

    return var_name, var_value

def parseArgs(argin,_vars):

    quoteChar = ""
    inQuote = False
    inArg = True
    args = []
    arg = ''
    
    for ch in argin.lstrip():
        if (inQuote == True):
            if (ch == quoteChar):
                inQuote = False   
                arg = arg + ch #z
            else:
                arg = arg + ch
        elif (ch == "\"" or ch == "\'"): # Do we have a quote
            quoteChar = ch
            arg = arg + ch #z
            inQuote = True
        elif (ch == " "):
            if (arg != ""):
                arg = subvars(arg,_vars)
                args.append(arg)
            else:
                args.append("null")
            arg = ""
        else:
            arg = arg + ch
                
    if (arg != ""):
        arg = subvars(arg,_vars)
        args.append(arg)   
               
    return(args)

def runMacro(script,in_sql,tokens):
    
    result = ""
    runIT = True 
    code = script.split("\n")
    level = 0
    runlevel = [True,False,False,False,False,False,False,False,False,False]
    ifcount = 0
    _vars = {}
    
    for i in range(0,len(tokens)):
        vstr = str(i)
        _vars[vstr] = tokens[i]
        
    if (len(tokens) == 0):
        _vars["argc"] = "0"
    else:
        _vars["argc"] = str(len(tokens)-1)
          
    for line in code:
        line = line.strip()
        if (line == "" or line == "\n"): continue
        if (line[0] == "#"): continue    # A comment line starts with a # in the first position of the line
        args = parseArgs(line,_vars)     # Get all of the arguments
        if (args[0] == "if"):
            ifcount = ifcount + 1
            if (runlevel[level] == False): # You can't execute this statement
                continue
            level = level + 1    
            if (len(args) < 4):
                print("Macro: Incorrect number of arguments for the if clause.")
                return insql
            arg1 = args[1]
            arg2 = args[3]
            if (len(arg2) > 2):
                ch1 = arg2[0]
                ch2 = arg2[-1:]
                if (ch1 in ['"',"'"] and ch1 == ch2):
                    arg2 = arg2[1:-1].strip()
               
            op   = args[2]
            if (op in ["=","=="]):
                if (arg1 == arg2):
                    runlevel[level] = True
                else:
                    runlevel[level] = False                
            elif (op in ["<=","=<"]):
                if (arg1 <= arg2):
                    runlevel[level] = True
                else:
                    runlevel[level] = False                
            elif (op in [">=","=>"]):                    
                if (arg1 >= arg2):
                    runlevel[level] = True
                else:
                    runlevel[level] = False                                       
            elif (op in ["<>","!="]):                    
                if (arg1 != arg2):
                    runlevel[level] = True
                else:
                    runlevel[level] = False  
            elif (op in ["<"]):
                if (arg1 < arg2):
                    runlevel[level] = True
                else:
                    runlevel[level] = False                
            elif (op in [">"]):
                if (arg1 > arg2):
                    runlevel[level] = True
                else:
                    runlevel[level] = False                
            else:
                print("Macro: Unknown comparison operator in the if statement:" + op)

                continue

        elif (args[0] in ["exit","echo"] and runlevel[level] == True):
            msg = ""
            for msgline in args[1:]:
                if (msg == ""):
                    msg = subvars(msgline,_vars)
                else:
                    msg = msg + " " + subvars(msgline,_vars)
            if (msg != ""): 
                if (args[0] == "echo"):
                    debug(msg,error=False)
                else:
                    debug(msg,error=True)
            if (args[0] == "exit"): return ''
       
        elif (args[0] == "pass" and runlevel[level] == True):
            pass

        elif (args[0] == "var" and runlevel[level] == True):
            value = ""
            for val in args[2:]:
                if (value == ""):
                    value = subvars(val,_vars)
                else:
                    value = value + " " + subvars(val,_vars)
            value.strip()
            _vars[args[1]] = value 

        elif (args[0] == 'else'):

            if (ifcount == level):
                runlevel[level] = not runlevel[level]
                
        elif (args[0] == 'return' and runlevel[level] == True):
            return(result)

        elif (args[0] == "endif"):
            ifcount = ifcount - 1
            if (ifcount < level):
                level = level - 1
                if (level < 0):
                    print("Macro: Unmatched if/endif pairs.")
                    return ''
                
        else:
            if (runlevel[level] == True):
                if (result == ""):
                    result = subvars(line,_vars)
                else:
                    result = result + "\n" + subvars(line,_vars)
                    
    return(result)       

def subvars(script,_vars):
    
    if (_vars == None): return script
    
    remainder = script
    result = ""
    done = False
    
    while done == False:
        bv = remainder.find("{")
        if (bv == -1):
            done = True
            continue
        ev = remainder.find("}")
        if (ev == -1):
            done = True
            continue
        result = result + remainder[:bv]
        vvar = remainder[bv+1:ev]
        remainder = remainder[ev+1:]
        
        upper = False
        allvars = False
        if (vvar[0] == "^"):
            upper = True
            vvar = vvar[1:]
        elif (vvar[0] == "*"):
            vvar = vvar[1:]
            allvars = True
        else:
            pass
        
        if (vvar in _vars):
            if (upper == True):
                items = _vars[vvar].upper()
            elif (allvars == True):
                try:
                    iVar = int(vvar)
                except:
                    return(script)
                items = ""
                sVar = str(iVar)
                while sVar in _vars:
                    if (items == ""):
                        items = _vars[sVar]
                    else:
                        items = items + " " + _vars[sVar]
                    iVar = iVar + 1
                    sVar = str(iVar)
            else:
                items = _vars[vvar]
        else:
            if (allvars == True):
                items = ""
            else:
                items = "null"                
                 
        result = result + items
                
    if (remainder != ""):
        result = result + remainder
        
    return(result)

def sqlTimer(hdbc, runtime, inSQL):
    
    count = 0
    t_end = time.time() + runtime
    
    while time.time() < t_end:
        
        try:
            stmt = ibm_db.exec_immediate(hdbc,inSQL) 
            if (stmt == False):
                db2_error(flag(["-q","-quiet"]))
                return(-1)
            ibm_db.free_result(stmt)
            
        except Exception as err:
            db2_error(False)
            return(-1)
        
        count = count + 1
                    
    return(count)

def splitargs(arguments):
    
    import types
    
    # String the string and remove the ( and ) characters if they at the beginning and end of the string
    
    results = []
    
    step1 = arguments.strip()
    if (len(step1) == 0): return(results)       # Not much to do here - no args found
    
    if (step1[0] == '('):
        if (step1[-1:] == ')'):
            step2 = step1[1:-1]
            step2 = step2.strip()
        else:
            step2 = step1
    else:
        step2 = step1
            
    # Now we have a string without brackets. Start scanning for commas
            
    quoteCH = ""
    pos = 0
    arg = ""
    args = []
            
    while pos < len(step2):
        ch = step2[pos]
        if (quoteCH == ""):                     # Are we in a quote?
            if (ch in ('"',"'")):               # Check to see if we are starting a quote
                quoteCH = ch
                arg = arg + ch
                pos += 1
            elif (ch == ","):                   # Are we at the end of a parameter?
                arg = arg.strip()
                args.append(arg)
                arg = ""
                inarg = False 
                pos += 1
            else:                               # Continue collecting the string
                arg = arg + ch
                pos += 1
        else:
            if (ch == quoteCH):                 # Are we at the end of a quote?
                arg = arg + ch                  # Add the quote to the string
                pos += 1                        # Increment past the quote
                quoteCH = ""                    # Stop quote checking (maybe!)
            else:
                pos += 1
                arg = arg + ch

    if (quoteCH != ""):                         # So we didn't end our string
        arg = arg.strip()
        args.append(arg)
    elif (arg != ""):                           # Something left over as an argument
        arg = arg.strip()
        args.append(arg)
    else:
        pass
    
    results = []
    
    for arg in args:
        result = []
        if (len(arg) > 0):
            if (arg[0] in ('"',"'")):
                value = arg[1:-1]
                isString = True
                isNumber = False
            else:
                isString = False 
                isNumber = False 
                try:
                    value = eval(arg)
                    if (type(value) == int):
                        isNumber = True
                    elif (isinstance(value,float) == True):
                        isNumber = True
                    else:
                        value = arg
                except:
                    value = arg

        else:
            value = ""
            isString = False
            isNumber = False
            
        result = [value,isString,isNumber]
        results.append(result)
        
    return results

def sqlParser(sqlin,local_ns):
       
    sql_cmd = ""
    encoded_sql = sqlin
    
    firstCommand = "(?:^\s*)([a-zA-Z]+)(?:\s+.*|$)"
    
    findFirst = re.match(firstCommand,sqlin)
    
    if (findFirst == None): # We did not find a match so we just return the empty string
        return sql_cmd, encoded_sql
    
    cmd = findFirst.group(1)
    sql_cmd = cmd.upper()

    #
    # Scan the input string looking for variables in the format :var. If no : is found just return.
    # Var must be alpha+number+_ to be valid
    #
    
    if (':' not in sqlin): # A quick check to see if parameters are in here, but not fool-proof!         
        return sql_cmd, encoded_sql    
    
    inVar = False 
    inQuote = "" 
    varName = ""
    encoded_sql = ""
    
    STRING = 0
    NUMBER = 1
    LIST = 2
    RAW = 3
    
    for ch in sqlin:
        if (inVar == True): # We are collecting the name of a variable
            if (ch.upper() in "@_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789[]"):
                varName = varName + ch
                continue
            else:
                if (varName == ""):
                    encode_sql = encoded_sql + ":"
                elif (varName[0] in ('[',']')):
                    encoded_sql = encoded_sql + ":" + varName
                else:
                    if (ch == '.'): # If the variable name is stopped by a period, assume no quotes are used
                        flag_quotes = False
                    else:
                        flag_quotes = True
                    varValue, varType = getContents(varName,flag_quotes,local_ns)
                    if (varValue == None):                 
                        encoded_sql = encoded_sql + ":" + varName
                    else:
                        if (varType == STRING):
                            encoded_sql = encoded_sql + varValue
                        elif (varType == NUMBER):
                            encoded_sql = encoded_sql + str(varValue)
                        elif (varType == RAW):
                            encoded_sql = encoded_sql + varValue
                        elif (varType == LIST):
                            start = True
                            for v in varValue:
                                if (start == False):
                                    encoded_sql = encoded_sql + ","
                                if (isinstance(v,int) == True):         # Integer value 
                                    encoded_sql = encoded_sql + str(v)
                                elif (isinstance(v,float) == True):
                                    encoded_sql = encoded_sql + str(v)
                                else:
                                    flag_quotes = True
                                    try:
                                        if (v.find('0x') == 0):               # Just guessing this is a hex value at beginning
                                            encoded_sql = encoded_sql + v
                                        else:
                                            encoded_sql = encoded_sql + addquotes(v,flag_quotes)      # String
                                    except:
                                        encoded_sql = encoded_sql + addquotes(str(v),flag_quotes)                                   
                                start = False

                encoded_sql = encoded_sql + ch
                varName = ""
                inVar = False  
        elif (inQuote != ""):
            encoded_sql = encoded_sql + ch
            if (ch == inQuote): inQuote = ""
        elif (ch in ("'",'"')):
            encoded_sql = encoded_sql + ch
            inQuote = ch
        elif (ch == ":"): # This might be a variable
            varName = ""
            inVar = True
        else:
            encoded_sql = encoded_sql + ch
    
    if (inVar == True):
        varValue, varType = getContents(varName,True,local_ns) # We assume the end of a line is quoted
        if (varValue == None):                 
            encoded_sql = encoded_sql + ":" + varName  
        else:
            if (varType == STRING):
                encoded_sql = encoded_sql + varValue
            elif (varType == NUMBER):
                encoded_sql = encoded_sql + str(varValue)
            elif (varType == LIST):
                flag_quotes = True
                start = True
                for v in varValue:
                    if (start == False):
                        encoded_sql = encoded_sql + ","
                    if (isinstance(v,int) == True):         # Integer value 
                        encoded_sql = encoded_sql + str(v)
                    elif (isinstance(v,float) == True):
                        encoded_sql = encoded_sql + str(v)
                    else:
                        try:
                            if (v.find('0x') == 0):               # Just guessing this is a hex value
                                encoded_sql = encoded_sql + v
                            else:
                                encoded_sql = encoded_sql + addquotes(v,flag_quotes)              # String
                        except:
                            encoded_sql = encoded_sql + addquotes(str(v),flag_quotes)                                 
                    start = False

    return sql_cmd, encoded_sql

def getContents(varName,flag_quotes,local_ns):
    
    #
    # Get the contents of the variable name that is passed to the routine. Only simple
    # variables are checked, i.e. arrays and lists are not parsed
    #
    
    STRING = 0
    NUMBER = 1
    LIST = 2
    RAW = 3
    DICT = 4
    
    try:
        value = eval(varName,None,local_ns) # globals()[varName] # eval(varName)
    except:
        return(None,STRING)
    
    if (isinstance(value,dict) == True):          # Check to see if this is JSON dictionary
        return(addquotes(value,flag_quotes),STRING)

    elif(isinstance(value,list) == True):         # List - tricky 
        return(value,LIST)

    elif (isinstance(value,int) == True):         # Integer value 
        return(value,NUMBER)

    elif (isinstance(value,float) == True):       # Float value
        return(value,NUMBER)

    else:
        try:
            # The pattern needs to be in the first position (0 in Python terms)
            if (value.find('0x') == 0):               # Just guessing this is a hex value
                return(value,RAW)
            else:
                return(addquotes(value,flag_quotes),STRING)                     # String
        except:
            return(addquotes(str(value),flag_quotes),RAW)

def addquotes(inString,flag_quotes):
    
    if (isinstance(inString,dict) == True):          # Check to see if this is JSON dictionary
        serialized = json.dumps(inString) 
    else:
        serialized = inString

    # Replace single quotes with '' (two quotes) and wrap everything in single quotes
    if (flag_quotes == False):
        return(serialized)
    else:
        return("'"+serialized.replace("'","''")+"'")    # Convert single quotes to two single quotes

def checkOption(args_in, option, vFalse=False, vTrue=True):
    
    args_out = args_in.strip()
    found = vFalse
    
    if (args_out != ""):
        if (args_out.find(option) >= 0):
            args_out = args_out.replace(option," ")
            args_out = args_out.strip()
            found = vTrue

    return args_out, found


def findProc(procname):
    
    global _hdbc, _hdbi, _connected, _runtime
    
    # Split the procedure name into schema.procname if appropriate
    upper_procname = procname.upper()
    schema, proc = split_string(upper_procname,".") # Expect schema.procname
    if (proc == None):
        proc = schema

    # Call ibm_db.procedures to see if the procedure does exist
    schema = "%"

    try:
        stmt = ibm_db.procedures(_hdbc, None, schema, proc) 
        if (stmt == False):                         # Error executing the code
            errormsg("Procedure " + procname + " not found in the system catalog.")
            return None

        result = ibm_db.fetch_tuple(stmt)
        resultsets = result[5]
        if (resultsets >= 1): resultsets = 1
        return resultsets
            
    except Exception as err:
        errormsg("Procedure " + procname + " not found in the system catalog.")
        return None

def getColumns(stmt):
    
    columns = []
    types = []
    colcount = 0
    try:
        colname = ibm_db.field_name(stmt,colcount)
        coltype = ibm_db.field_type(stmt,colcount)
        while (colname != False):
            columns.append(colname)
            types.append(coltype)
            colcount += 1
            colname = ibm_db.field_name(stmt,colcount)
            coltype = ibm_db.field_type(stmt,colcount)            
        return columns,types   
                
    except Exception as err:
        db2_error(False)
        return None

def parseCall(hdbc, inSQL, local_ns):
    
    global _hdbc, _hdbi, _connected, _runtime, _environment
 
    # Check to see if we are connected first
    if (_connected == False):                                      # Check if you are connected 
        db2_doConnect()
        if _connected == False: return None
    
    remainder = inSQL.strip()
    procName, procArgs = parseCallArgs(remainder[5:]) # Assume that CALL ... is the format

    resultsets = findProc(procName)
    if (resultsets == None): return None
    
    argvalues = []
 
    if (len(procArgs) > 0): # We have arguments to consider
        for arg in procArgs:
            varname = arg[1]
            if (len(varname) > 0):
                if (varname[0] == ":"):
                    checkvar = varname[1:]
                    varvalue = getContents(checkvar,True,local_ns)
                    if (varvalue == None):
                        errormsg("Variable " + checkvar + " is not defined.")
                        return None
                    argvalues.append(varvalue)
                else:
                    if (varname.upper() == "NULL"):
                        argvalues.append(None)
                    else:
                        argvalues.append(varname)
            else:
                if (varname.upper() == "NULL"):
                    argvalues.append(None)
                else:
                    argvalues.append(varname)                
    
    try:

        if (len(procArgs) > 0):
            argtuple = tuple(argvalues)
            result = ibm_db.callproc(_hdbc,procName,argtuple)
            stmt = result[0]
        else:
            result = ibm_db.callproc(_hdbc,procName)
            stmt = result
        
        if (resultsets == 1 and stmt != None):

            columns, types = getColumns(stmt)
            if (columns == None): return None
            
            rows = []
            rowlist = ibm_db.fetch_tuple(stmt)
            while ( rowlist ) :
                row = []
                colcount = 0
                for col in rowlist:
                    try:
                        if (types[colcount] in ["int","bigint"]):
                            row.append(int(col))
                        elif (types[colcount] in ["decimal","real"]):
                            row.append(float(col))
                        elif (types[colcount] in ["date","time","timestamp"]):
                            row.append(str(col))
                        else:
                            row.append(col)
                    except:
                        row.append(col)
                    colcount += 1
                rows.append(row)
                rowlist = ibm_db.fetch_tuple(stmt)
            
            if flag(["-r","-array"]):
                rows.insert(0,columns)
                if len(procArgs) > 0:
                    allresults = []
                    allresults.append(rows)
                    for x in result[1:]:
                        allresults.append(x)
                    return allresults # rows,returned_results
                else:
                    return rows
            else:
                df = pandas.DataFrame.from_records(rows,columns=columns)
                if flag("-grid") or _settings['display'] == 'GRID':
                    if (_environment['qgrid'] == False):
                        with pandas.option_context('display.max_rows', None, 'display.max_columns', None):  
                            pdisplay(df)
                    else:
                        try:
                            pdisplay(qgrid.show_grid(df))
                        except:
                            errormsg("Grid cannot be used to display data with duplicate column names. Use option -a or %sql OPTION DISPLAY PANDAS instead.")
                            
                    return                             
                else:
                    if flag(["-a","-all"]) or _settings["maxrows"] == -1 : # All of the rows
                        with pandas.option_context('display.max_rows', None, 'display.max_columns', None): 
                            pdisplay(df)
                    else:
                        return df
            
        else:
            if len(procArgs) > 0:
                allresults = []
                for x in result[1:]:
                    allresults.append(x)
                return allresults # rows,returned_results
            else:
                return None
            
    except Exception as err:
        db2_error(False)
        return None

def parsePExec(hdbc, inSQL):
     
    import ibm_db    
    global _stmt, _stmtID, _stmtSQL, sqlcode
    
    cParms = inSQL.split()
    parmCount = len(cParms)
    if (parmCount == 0): return(None)                          # Nothing to do but this shouldn't happen
    
    keyword = cParms[0].upper()                                  # Upper case the keyword
    
    if (keyword == "PREPARE"):                                   # Prepare the following SQL
        uSQL = inSQL.upper()
        found = uSQL.find("PREPARE")
        sql = inSQL[found+7:].strip()

        try:
            pattern = "\?\*[0-9]+"
            findparm = re.search(pattern,sql)
            while findparm != None:
                found = findparm.group(0)
                count = int(found[2:])
                markers = ('?,' * count)[:-1]
                sql = sql.replace(found,markers)
                findparm = re.search(pattern,sql)
            
            stmt = ibm_db.prepare(hdbc,sql) # Check error code here
            if (stmt == False): 
                db2_error(False)
                return(False)
            
            stmttext = str(stmt).strip()
            stmtID = stmttext[33:48].strip()
            
            if (stmtID in _stmtID) == False:
                _stmt.append(stmt)              # Prepare and return STMT to caller
                _stmtID.append(stmtID)
            else:
                stmtIX = _stmtID.index(stmtID)
                _stmt[stmtiX] = stmt
                 
            return(stmtID)
        
        except Exception as err:
            print(err)
            db2_error(False)
            return(False)

    if (keyword == "EXECUTE"):                                  # Execute the prepare statement
        if (parmCount < 2): return(False)                    # No stmtID available
        
        stmtID = cParms[1].strip()
        if (stmtID in _stmtID) == False:
            errormsg("Prepared statement not found or invalid.")
            return(False)

        stmtIX = _stmtID.index(stmtID)
        stmt = _stmt[stmtIX]

        try:        

            if (parmCount == 2):                           # Only the statement handle available
                result = ibm_db.execute(stmt)               # Run it
            elif (parmCount == 3):                          # Not quite enough arguments
                errormsg("Missing or invalid USING clause on EXECUTE statement.")
                sqlcode = -99999
                return(False)
            else:
                using = cParms[2].upper()
                if (using != "USING"):                     # Bad syntax again
                    errormsg("Missing USING clause on EXECUTE statement.")
                    sqlcode = -99999
                    return(False)
                
                uSQL = inSQL.upper()
                found = uSQL.find("USING")
                parmString = inSQL[found+5:].strip()
                parmset = splitargs(parmString)
 
                if (len(parmset) == 0):
                    errormsg("Missing parameters after the USING clause.")
                    sqlcode = -99999
                    return(False)
                    
                parms = []

                parm_count = 0
                
                CONSTANT = 0
                VARIABLE = 1
                const = [0]
                const_cnt = 0
                
                for v in parmset:
                    
                    parm_count = parm_count + 1
                    
                    if (v[1] == True or v[2] == True): # v[1] true if string, v[2] true if num
                        
                        parm_type = CONSTANT                        
                        const_cnt = const_cnt + 1
                        if (v[2] == True):
                            if (isinstance(v[0],int) == True):         # Integer value 
                                sql_type = ibm_db.SQL_INTEGER
                            elif (isinstance(v[0],float) == True):       # Float value
                                sql_type = ibm_db.SQL_DOUBLE
                            else:
                                sql_type = ibm_db.SQL_INTEGER
                        else:
                            sql_type = ibm_db.SQL_CHAR
                        
                        const.append(v[0])

                        
                    else:
                    
                        parm_type = VARIABLE
                    
                        # See if the variable has a type associated with it varname@type
                    
                        varset = v[0].split("@")
                        parm_name = varset[0]
                        
                        parm_datatype = "char"

                        # Does the variable exist?
                        if (parm_name not in globals()):
                            errormsg("SQL Execute parameter " + parm_name + " not found")
                            sqlcode = -99999
                            return(false)                        
        
                        if (len(varset) > 1):                # Type provided
                            parm_datatype = varset[1]

                        if (parm_datatype == "dec" or parm_datatype == "decimal"):
                            sql_type = ibm_db.SQL_DOUBLE
                        elif (parm_datatype == "bin" or parm_datatype == "binary"):
                            sql_type = ibm_db.SQL_BINARY
                        elif (parm_datatype == "int" or parm_datatype == "integer"):
                            sql_type = ibm_db.SQL_INTEGER
                        else:
                            sql_type = ibm_db.SQL_CHAR
                    
                    try:
                        if (parm_type == VARIABLE):
                            result = ibm_db.bind_param(stmt, parm_count, globals()[parm_name], ibm_db.SQL_PARAM_INPUT, sql_type)
                        else:
                            result = ibm_db.bind_param(stmt, parm_count, const[const_cnt], ibm_db.SQL_PARAM_INPUT, sql_type)
                            
                    except:
                        result = False
                        
                    if (result == False):
                        errormsg("SQL Bind on variable " + parm_name + " failed.")
                        sqlcode = -99999
                        return(false) 
                    
                result = ibm_db.execute(stmt) # ,tuple(parms))
                
            if (result == False): 
                errormsg("SQL Execute failed.")      
                return(False)
            
            if (ibm_db.num_fields(stmt) == 0): return(True) # Command successfully completed
                          
            return(fetchResults(stmt))
                        
        except Exception as err:
            db2_error(False)
            return(False)
        
        return(False)
  
    return(False)     

def fetchResults(stmt):
     
    global sqlcode
    
    rows = []
    columns, types = getColumns(stmt)
    
    # By default we assume that the data will be an array
    is_array = True
    
    # Check what type of data we want returned - array or json
    if (flag(["-r","-array"]) == False):
        # See if we want it in JSON format, if not it remains as an array
        if (flag("-json") == True):
            is_array = False
    
    # Set column names to lowercase for JSON records
    if (is_array == False):
        columns = [col.lower() for col in columns] # Convert to lowercase for each of access
    
    # First row of an array has the column names in it
    if (is_array == True):
        rows.append(columns)
        
    result = ibm_db.fetch_tuple(stmt)
    rowcount = 0
    while (result):
        
        rowcount += 1
        
        if (is_array == True):
            row = []
        else:
            row = {}
            
        colcount = 0
        for col in result:
            try:
                if (types[colcount] in ["int","bigint"]):
                    if (is_array == True):
                        row.append(int(col))
                    else:
                        row[columns[colcount]] = int(col)
                elif (types[colcount] in ["decimal","real"]):
                    if (is_array == True):
                        row.append(float(col))
                    else:
                        row[columns[colcount]] = float(col)
                elif (types[colcount] in ["date","time","timestamp"]):
                    if (is_array == True):
                        row.append(str(col))
                    else:
                        row[columns[colcount]] = str(col)
                else:
                    if (is_array == True):
                        row.append(col)
                    else:
                        row[columns[colcount]] = col
                        
            except:
                if (is_array == True):
                    row.append(col)
                else:
                    row[columns[colcount]] = col
                    
            colcount += 1
        
        rows.append(row)
        result = ibm_db.fetch_tuple(stmt)
        
    if (rowcount == 0): 
        sqlcode = 100        
    else:
        sqlcode = 0
        
    return rows
            

def parseCommit(sql):
    
    global _hdbc, _hdbi, _connected, _runtime, _stmt, _stmtID, _stmtSQL

    if (_connected == False): return                        # Nothing to do if we are not connected
    
    cParms = sql.split()
    if (len(cParms) == 0): return                           # Nothing to do but this shouldn't happen
    
    keyword = cParms[0].upper()                             # Upper case the keyword
    
    if (keyword == "COMMIT"):                               # Commit the work that was done
        try:
            result = ibm_db.commit (_hdbc)                  # Commit the connection
            if (len(cParms) > 1):
                keyword = cParms[1].upper()
                if (keyword == "HOLD"):
                    return
            
            del _stmt[:]
            del _stmtID[:]

        except Exception as err:
            db2_error(False)
        
        return
        
    if (keyword == "ROLLBACK"):                             # Rollback the work that was done
        try:
            result = ibm_db.rollback(_hdbc)                  # Rollback the connection
            del _stmt[:]
            del _stmtID[:]            

        except Exception as err:
            db2_error(False)
        
        return
    
    if (keyword == "AUTOCOMMIT"):                           # Is autocommit on or off
        if (len(cParms) > 1): 
            op = cParms[1].upper()                          # Need ON or OFF value
        else:
            return
        
        try:
            if (op == "OFF"):
                ibm_db.autocommit(_hdbc, False)
            elif (op == "ON"):
                ibm_db.autocommit (_hdbc, True)
            return    
        
        except Exception as err:
            db2_error(False)
            return 
        
    return

def setFlags(inSQL):
    
    global _flags
    
    _flags = [] # Delete all of the current flag settings
    
    pos = 0
    end = len(inSQL)-1
    inFlag = False
    ignore = False
    outSQL = ""
    flag = ""
    
    while (pos <= end):
        ch = inSQL[pos]
        if (ignore == True):   
            outSQL = outSQL + ch
        else:
            if (inFlag == True):
                if (ch != " "):
                    flag = flag + ch
                else:
                    _flags.append(flag)
                    inFlag = False
            else:
                if (ch == "-"):
                    flag = "-"
                    inFlag = True
                elif (ch == ' '):
                    outSQL = outSQL + ch
                else:
                    outSQL = outSQL + ch
                    ignore = True
        pos += 1
        
    if (inFlag == True):
        _flags.append(flag)
        
    return outSQL

def flag(inflag):
    
    global _flags

    if isinstance(inflag,list):
        for x in inflag:
            if (x in _flags):
                return True
        return False
    else:
        if (inflag in _flags):
            return True
        else:
            return False

def splitSQL(inputString, delimiter):
     
    pos = 0
    arg = ""
    results = []
    quoteCH = ""
    
    inSQL = inputString.strip()
    if (len(inSQL) == 0): return(results)       # Not much to do here - no args found
            
    while pos < len(inSQL):
        ch = inSQL[pos]
        pos += 1
        if (ch in ('"',"'")):                   # Is this a quote characters?
            arg = arg + ch                      # Keep appending the characters to the current arg
            if (ch == quoteCH):                 # Is this quote character we are in
                quoteCH = ""
            elif (quoteCH == ""):               # Create the quote
                quoteCH = ch
            else:
                None
        elif (quoteCH != ""):                   # Still in a quote
            arg = arg + ch
        elif (ch == delimiter):                 # Is there a delimiter?
            results.append(arg)
            arg = ""
        else:
            arg = arg + ch
            
    if (arg != ""):
        results.append(arg)
        
    return(results)

@magics_class
class DB2(Magics):
   
    @needs_local_scope    
    @line_cell_magic
    def sql(self, line, cell=None, local_ns=None):
            
        # Before we event get started, check to see if you have connected yet. Without a connection we 
        # can't do anything. You may have a connection request in the code, so if that is true, we run those,
        # otherwise we connect immediately
        
        # If your statement is not a connect, and you haven't connected, we need to do it for you
    
        global _settings, _environment
        global _hdbc, _hdbi, _connected, _runtime, sqlstate, sqlerror, sqlcode, sqlelapsed
             
        # If you use %sql (line) we just run the SQL. If you use %%SQL the entire cell is run.
        
        flag_cell = False
        flag_output = False
        sqlstate = "0"
        sqlerror = ""
        sqlcode = 0
        sqlelapsed = 0
        
        start_time = time.time()
        end_time = time.time()
              
        # Macros gets expanded before anything is done
                
        SQL1 = setFlags(line.strip())  
        SQL1 = checkMacro(SQL1)                                   # Update the SQL if any macros are in there
        SQL2 = cell    
        
        if flag("-sampledata"):                                   # Check if you only want sample data loaded
            if (_connected == False):
                if (db2_doConnect() == False):
                    errormsg('A CONNECT statement must be issued before issuing SQL statements.')
                    return                              
                
            db2_create_sample(flag(["-q","-quiet"]))
            return  
        
        if SQL1 == "?" or flag(["-h","-help"]):                   # Are you asking for help
            sqlhelp()
            return
        
        if len(SQL1) == 0 and SQL2 == None: return                # Nothing to do here
                
        # Check for help

        if SQL1.upper() == "? CONNECT":                           # Are you asking for help on CONNECT
            connected_help()
            return        
        
        sqlType,remainder = sqlParser(SQL1,local_ns)              # What type of command do you have?
                
        if (sqlType == "CONNECT"):                                # A connect request 
            parseConnect(SQL1,local_ns)
            return
        elif (sqlType == "DEFINE"):                               # Create a macro from the body
            result = setMacro(SQL2,remainder)
            return
        elif (sqlType == "OPTION"):
            setOptions(SQL1)
            return 
        elif (sqlType == 'COMMIT' or sqlType == 'ROLLBACK' or sqlType == 'AUTOCOMMIT'):
            parseCommit(remainder)
            return
        elif (sqlType == "PREPARE"):
            pstmt = parsePExec(_hdbc, remainder)
            return(pstmt)
        elif (sqlType == "EXECUTE"):
            result = parsePExec(_hdbc, remainder)
            return(result)    
        elif (sqlType == "CALL"):
            result = parseCall(_hdbc, remainder, local_ns)
            return(result)
        else:
            pass        
 
        sql = SQL1
    
        if (sql == ""): sql = SQL2
        
        if (sql == ""): return                                   # Nothing to do here
    
        if (_connected == False):
            if (db2_doConnect() == False):
                errormsg('A CONNECT statement must be issued before issuing SQL statements.')
                return      
        
        if _settings["maxrows"] == -1:                                 # Set the return result size
            pandas.reset_option('display.max_rows')
        else:
            pandas.options.display.max_rows = _settings["maxrows"]
      
        runSQL = re.sub('.*?--.*$',"",sql,flags=re.M)
        remainder = runSQL.replace("\n"," ") 
        if flag(["-d","-delim"]):
            sqlLines = splitSQL(remainder,"@")
        else:
            sqlLines = splitSQL(remainder,";")
        flag_cell = True
                      
        # For each line figure out if you run it as a command (db2) or select (sql)

        for sqlin in sqlLines:          # Run each command
            
            sqlin = checkMacro(sqlin)                                 # Update based on any macros

            sqlType, sql = sqlParser(sqlin,local_ns)                           # Parse the SQL  
            if (sql.strip() == ""): continue
            if flag(["-e","-echo"]): debug(sql,False)
                
            if flag("-t"):
                cnt = sqlTimer(_hdbc, _settings["runtime"], sql)            # Given the sql and parameters, clock the time
                if (cnt >= 0): print("Total iterations in %s second(s): %s" % (_settings["runtime"],cnt))                
                return(cnt)
 
            else:
        
                try:                                                  # See if we have an answer set
                    stmt = ibm_db.prepare(_hdbc,sql)
                    if (ibm_db.num_fields(stmt) == 0):                # No, so we just execute the code
                        result = ibm_db.execute(stmt)                 # Run it                            
                        if (result == False):                         # Error executing the code
                            db2_error(flag(["-q","-quiet"])) 
                            continue
                            
                        rowcount = ibm_db.num_rows(stmt)    
                    
                        if (rowcount == 0 and flag(["-q","-quiet"]) == False):
                            errormsg("No rows found.")     
                            
                        continue                                      # Continue running
                    
                    elif flag(["-r","-array","-j","-json"]):                     # raw, json, format json
                        row_count = 0
                        resultSet = []
                        try:
                            result = ibm_db.execute(stmt)             # Run it
                            if (result == False):                         # Error executing the code
                                db2_error(flag(["-q","-quiet"]))  
                                return
                                
                            if flag("-j"):                          # JSON single output
                                row_count = 0
                                json_results = []
                                while( ibm_db.fetch_row(stmt) ):
                                    row_count = row_count + 1
                                    jsonVal = ibm_db.result(stmt,0)
                                    jsonDict = json.loads(jsonVal)
                                    json_results.append(jsonDict)
                                    flag_output = True                                    
                                
                                if (row_count == 0): sqlcode = 100
                                return(json_results)
                            
                            else:
                                return(fetchResults(stmt))
                                  
                        except Exception as err:
                            db2_error(flag(["-q","-quiet"]))
                            return
                            
                    else:
                        
                        try:
                            df = pandas.read_sql(sql,_hdbi)
          
                        except Exception as err:
                            db2_error(False)
                            return
                    
                        if (len(df) == 0):
                            sqlcode = 100
                            if (flag(["-q","-quiet"]) == False): 
                                errormsg("No rows found")
                            continue                    
                    
                        flag_output = True
                        if flag("-grid") or _settings['display'] == 'GRID':   # Check to see if we can display the results
                            if (_environment['qgrid'] == False):
                                with pandas.option_context('display.max_rows', None, 'display.max_columns', None):  
                                    print(df.to_string())
                            else:
                                try:
                                    pdisplay(qgrid.show_grid(df))
                                except:
                                    errormsg("Grid cannot be used to display data with duplicate column names. Use option -a or %sql OPTION DISPLAY PANDAS instead.")
                                    return 
                        else:
                            if flag(["-a","-all"]) or _settings["maxrows"] == -1 : # All of the rows
                                pandas.options.display.max_rows = None
                                pandas.options.display.max_columns = None
                                return df # print(df.to_string())
                            else:
                                pandas.options.display.max_rows = _settings["maxrows"]
                                pandas.options.display.max_columns = None
                                return df # pdisplay(df) # print(df.to_string())
 
                except:
                    db2_error(flag(["-q","-quiet"]))
                    continue # return
                
        end_time = time.time()
        sqlelapsed = end_time - start_time
        if (flag_output == False and flag(["-q","-quiet"]) == False): print("Command completed.")
            
# Register the Magic extension in Jupyter    
ip = get_ipython()          
ip.register_magics(DB2)
load_settings()
   
success("Db2 Extensions Loaded.")