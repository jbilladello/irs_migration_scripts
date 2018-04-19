# For the State IRS Migration data from 2004-2013, containing .dat and .csv files

import csv, sqlite3, os

State_data = os.path.join('C:\\Users\\JBilladello\\Desktop\\irs_taxdata\\state\\State2004-2013')

widths=[2,1,3,1,2,1,3,1,2,1,32,1,9,1,10,1,12] # Width of all fields in state .dat file 

in_rows = []

# Trick for generating input fields for database

def sumdict(theval,thedict):
    if theval in thedict:
        thedict[theval]=thedict[theval]+1
    else:
        thedict[theval]=1

fieldnum=8
qmark='?,'*fieldnum
newmark=qmark[:-1]

# Create database and data fields
con = sqlite3.connect('State_0413_test.sqlite')
cur = con.cursor()

for files in os.listdir(State_data):
    if files [0:7]=='statein':
        id_codes = {}
        in_years = files[-8:-4]     # years for the inflows
        with open(files, 'rU') as state_file:
            parsedlist=[]           # To be filled with 'widths' spacing designations
            newlist=[]
            if files [-4:]=='.dat':
                for line in state_file:
                    idx=0
                    line_list=[]
                    for i in widths:
                        increment=idx+i
                        line_list.append(line[idx:increment].strip())
                        idx=increment
                    parsedlist.append(line_list)
                    for record in parsedlist:
                        temp_list=[]
                        for item in record: 
                            if item != '':               # != does not equal
                                temp_list.append(item)   # removes blank spaces from parsedlist and appends to temp_list
                        del temp_list[3]
                        del temp_list[1]
                        State_Origin = temp_list[3].title()
                        temp_list.insert(3, State_Origin)
                        del temp_list[4]
                        unique_id = temp_list[0] + '_' + temp_list[1]   #creates Unique ID 
                        temp_list.insert(0,unique_id)
                    newlist.append(temp_list)       # deletes items in positions 3 and 1 in temp_list and creates newlist

                else:
                    pass
                
            elif files [-4:]=='.csv':
                headers = state_file.readline().strip().split(',') # Skips the first line
                for line in state_file:
                    line = line.strip().split(',') # Takes every line in each file and appends it to newlist
                    State_Origin = line[3].title()
                    line.insert(3, State_Origin)
                    del line[4]
                    unique_id = line[0] + '_' + line[1]
                    line.insert(0,unique_id)
                    newlist.append(line)

            header_in = 'inflow'+ in_years

            cur.execute("DROP TABLE IF EXISTS %s" %header_in)

            cur.execute(""" 
            CREATE TABLE %s (
            Unique_ID TEXT,
            State_FIPS_Destination TEXT,
            State_FIPS_Origin TEXT,
            State_Abbrv_Origin TEXT,
            State_Origin TEXT,
            Number_Returns INTEGER,
            Number_Exemptions INTEGER,
            Adjusted_Gross_Income INTEGER)"""
            %header_in)         # Creates State in-flow data table

            cur.executemany('INSERT INTO %s VALUES(%s)'%(header_in,newmark), newlist)
            
        print(cur.rowcount,'Records were written to the State in-flow table') # Should have 2808 per table, except 1112.csv and 1213.csv
        con.commit()
                
    elif files [0:8]=='stateout':
        out_years = files[-8:-4] # years for the outflows
        with open(files, 'rU') as dat_file:
            parsedlist=[] 
            newlist=[]
            if files [-4:]=='.dat':
                for line in dat_file:
                    idx=0
                    line_list=[]
                    for i in widths:
                        increment=idx+i
                        line_list.append(line[idx:increment].strip())
                        idx=increment
                    parsedlist.append(line_list)
                    for record in parsedlist:
                        temp_list=[]
                        for item in record: 
                            if item != '':
                                temp_list.append(item) # removes blank spaces from parsedlist and appends to temp_list
                        del temp_list[3]
                        del temp_list[1]
                        State_Dest = temp_list[3].title()
                        temp_list.insert(3, State_Dest)
                        del temp_list[4]
                        unique_id = temp_list[0] + '_' + temp_list[1] #creates Unique ID 
                        temp_list.insert(0,unique_id)
                    newlist.append(temp_list) #deletes items in positions 3 and 1 in temp_list and creates newlist

                else:
                    pass
                
            elif files [-4:]=='.csv':
                headers = dat_file.readline().strip().split(',') # Skips the first line
                for line in dat_file:
                    line = line.strip().split(',') # Takes every line in each file and appends it to newlist
                    State_Dest = line[3].title()
                    line.insert(3, State_Dest)
                    del line[4]
                    unique_id = line[0] + '_' + line[1]
                    line.insert(0,unique_id)
                    newlist.append(line)

            header_out = 'outflow'+ out_years

            cur.execute("DROP TABLE IF EXISTS %s" %header_out)

            cur.execute("""
            CREATE TABLE %s (
            Unique_ID TEXT,
            State_FIPS_Origin TEXT,
            State_FIPS_Destination TEXT,
            State_Abbrv_Destination TEXT,
            State_Destination TEXT,
            Number_Returns INTEGER,
            Number_Exemptions INTEGER,
            Adjusted_Gross_Income INTEGER)"""
            %header_out)        # Creates State out-flow data table

            cur.executemany('INSERT INTO %s VALUES(%s)'%(header_out,newmark), newlist)

        print(cur.rowcount,'Records were written to the State out-flow table')
        con.commit()
    
con.close()            

print('All files have been processed and exported to IRS_Migration database')
