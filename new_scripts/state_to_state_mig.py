# This script works with CSV files for State-level IRS migration data from the years 2013 - 2014 forward.
# It loops through the state inflow and state outflow files, formats their rows into a list,
# creates summary tables for 'totals' records, and exports the lists to an SQLite database.

# Users will be prompted to enter the full path to the folder in which the data files are stored,
# confirm that the table names are accurate for the year being processed, name the SQLite database.
# The database is intended to be a test container, where data can be scrutinized prior to wrtiting it to
# a final database, and is created in the same folder as the source files.
# As a final step, the script runs through each data table and separates suppressed records
# from the returns and exemptions columns into a new 'disclosure' column, where they appear as a -1.

#If the script returns an error message that writing to the database has failed due to duplicate records,
#remove the PRIMARY KEY contraint from the create table statements, re-run the script to get output,
#and scrutinize the resulting records to determine why there are duplicate keys.

import sqlite3, os
# enter path to folders where new data is stored first
path_to_state_data = os.path.join(input('Enter path to folder where new data is stored: '))

fieldnum = 9
qmark='?,'*fieldnum
newmark=qmark[:-1]

# this is a dictionary of states and their FIPS codes
stcode = {'01':'AL', '02':'AK', '04':'AZ', '05':'AR', '06':'CA', '08':'CO', '09':'CT', '10':'DE', '11':'DC', '12':'FL',
      '13':'GA', '15':'HI', '16':'ID', '17':'IL', '18':'IN', '19':'IA', '20':'KS', '21':'KY', '22':'LA', '23':'ME',
      '24':'MD', '25':'MA', '26':'MI', '27':'MN', '28':'MS', '29':'MO', '30':'MT', '31':'NE', '32':'NV', '33':'NH',
      '34':'NJ', '35':'NM', '36':'NY', '37':'NC', '38':'ND', '39':'OH', '40':'OK', '41':'OR', '42':'PA', '44':'RI',
      '45':'SC', '46':'SD', '47':'TN', '48':'TX', '49':'UT', '50':'VT', '51':'VA', '53':'WA', '54':'WV', '55':'WI',
      '56':'WY', '57':'FR', '58':'SS', '59':'DS'}

for item in os.listdir(path_to_state_data):
    if item [0:7] == 'statein':
        infilepath=os.path.join(path_to_state_data,item)
        in_years = item[-8:-4]      #years for the inflow
        header_in = 'inflow_20' + in_years[0:2] + '_' + in_years[2:4]
        totals_in = header_in + "_totals"
        print('Inflow table name: ' + header_in)
        print('Totals inflow table name: ' + totals_in)
        print('Add this to the end of the path for Inflow: ' + item)

    elif item [0:8] == 'stateout':
        outfilepath=os.path.join(path_to_state_data,item)
        out_years = item[-8:-4]     #years for the outflow
        header_out = 'outflow_20' + out_years[0:2] + '_' + out_years[2:4]
        totals_out = header_out + '_totals'
        print('Outflow table name: ' + header_out)
        print('Totals outflow table name: ' + totals_out)
        print('Add this to the end of the path for Outflow: ' + item)

confirm = input('Do the table titles match the years you are creating data for? y/n: ')
if confirm == 'y':
    print('Great, name the SQL database where data will be stored: ')      
elif confirm == 'n':
    print('Exit and run script again with correct folder name')

db_name = os.path.join(path_to_state_data,input()) + '.sqlite'
con = sqlite3.connect(db_name)
cur = con.cursor()
print('Created SQLite database: ' + db_name)

#--inflow--#

with open(infilepath) as states:     
        newlist_in = []
        newlist_totals = []
        headers = states.readline().strip().split(',') #skips first line
        for line in states:
            line = line.strip().split(',')
            unique_id = line[0] + '_' + line[1]
            line.insert(0,unique_id)
            state_dest = line[1]        #checks dictionary for FIPS code and replaces it with state abbrv.
            for k, v in stcode.items():
                if state_dest in k:
                    st_abbrv = str(v)
            line.insert(1,st_abbrv)
            if line[3] == '96':         #separate out Totals headers
                newlist_totals.append(line)
            elif line[3] == '97' and line[5].endswith('-US'):
                newlist_totals.append(line)
            elif line[3] == '98':
                newlist_totals.append(line)
            else:
                newlist_in.append(line)
        #print(newlist_totals)
        print('Read in State Inflow records')

        cur.execute("DROP TABLE IF EXISTS %s" %totals_in)

        cur.execute("""
        CREATE TABLE %s (
        uid TEXT NOT NULL PRIMARY KEY,
        st_dest_abbrv TEXT,
        destination TEXT,
        origin TEXT,
        st_orig_abbrv TEXT,
        st_orig_name TEXT,
        returns INTEGER,
        exemptions INTEGER,
        income INTEGER)"""
        %totals_in)            # Creates State inflow totals table

        cur.executemany('INSERT INTO %s VALUES(%s)'%(totals_in,newmark), newlist_totals)
        print(cur.rowcount, 'Records were written to the State inflow totals table')

        cur.execute("DROP TABLE IF EXISTS %s" %header_in)

        cur.execute(""" 
        CREATE TABLE %s (
        uid TEXT NOT NULL PRIMARY KEY,
        st_dest_abbrv TEXT,
        destination TEXT,
        origin TEXT,
        st_orig_abbrv TEXT,
        st_orig_name TEXT,
        returns INTEGER,
        exemptions INTEGER,
        income INTEGER)"""  
        %header_in)         # Creates State inflow data table

        cur.executemany('INSERT INTO %s VALUES(%s)'%(header_in,newmark), newlist_in)

        print(cur.rowcount, 'Records were written to the State inflow table')

        con.commit()

#--outflow--#
with open(outfilepath) as states:
    newlist_out = []
    newlist_totals_out = []
    headers = states.readline().strip().split(',')
    for line in states:
        line = line.strip().split(',')
        unique_id = line[0] + '_' + line[1]
        line.insert(0,unique_id)
        state_orig = line[1]        #checks dictionary for FIPS code and replaces it with state abbrv.
        for k, v in stcode.items():
            if state_orig in k:
                st_abbrv = str(v)
        line.insert(1,st_abbrv)
        if line[3] == '96':         #separate out Totals headers
            newlist_totals_out.append(line)
        elif line[3] == '97' and line[5].endswith('-US'):
            newlist_totals_out.append(line)
        elif line[3] == '98':
            newlist_totals_out.append(line)
        else:
            newlist_out.append(line)

    cur.execute("DROP TABLE IF EXISTS %s" %totals_out)

    cur.execute("""
    CREATE TABLE %s (
    uid TEXT NOT NULL PRIMARY KEY,
    st_orig_abbrv TEXT,
    origin TEXT,
    destination TEXT,
    st_dest_abbrv TEXT,
    st_dest_name TEXT,
    returns INTEGER,
    exemptions INTEGER,
    income INTEGER)"""
    %totals_out)            # Creates State outflow totals table

    cur.executemany('INSERT INTO %s VALUES(%s)'%(totals_out,newmark), newlist_totals_out)
    print(cur.rowcount, 'Records were written to the State outflow totals table')
            
    cur.execute("DROP TABLE IF EXISTS %s" %header_out)

    cur.execute("""
    CREATE TABLE %s (
    uid TEXT NOT NULL PRIMARY KEY,
    st_orig_abbrv TEXT,
    origin TEXT,
    destination TEXT,
    st_dest_abbrv TEXT,
    st_dest_name TEXT,
    returns INTEGER,
    exemptions INTEGER,
    income INTEGER)"""  
    %header_out)        # Creates State outflow data table

    cur.executemany('INSERT INTO %s VALUES(%s)'%(header_out,newmark), newlist_out)

    print(cur.rowcount, 'Records were written to the State outflow table')
    con.commit()

# Add footnote column for disclosure, and remove footnotes from data columns

cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tabnames = cur.fetchall()
for tab in tabnames:
    if tab[0].startswith('inflow') or tab[0].startswith('outflow'):
        cur.execute("ALTER TABLE %s ADD COLUMN disclosure INTEGER;" %tab)           #drop if exists
        cur.execute("UPDATE %s SET disclosure =-1 WHERE returns IN (-1,'d');" %tab)
        cur.execute("UPDATE %s SET returns = NULL WHERE disclosure IN (-1,'d');" %tab)
        cur.execute("UPDATE %s SET exemptions = NULL WHERE disclosure IN (-1,'d');" %tab)
        cur.execute("UPDATE %s SET income = NULL WHERE disclosure IN (-1,'d');" %tab)
        con.commit()
        print("Added columns for " + tab[0])

con.close()            

print('All files have been processed and exported to IRS Migration database')    


