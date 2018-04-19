import os, sqlite3, csv

# For the County folder in irs_taxdata, County2004-2013, containing .dat and .csv

# Generates input fields for database
fieldnum=9
qmark='?,'*fieldnum
newmark=qmark[:-1]

#Sets widths of columns 
widths = [2,1,3,1,2,1,3,1,2,1,32,1,9,1,10,1,12]
in_rows = []

con = sqlite3.connect('County_test0413.sqlite')
con.text_factory = str
cur = con.cursor()

dir_name = os.getcwd()

for item in os.listdir(dir_name):
    if item [0:8]=='countyin':
        in_years = item[-8:-4]
        with open(item,'rU') as county_file:
            parsedlist = []
            newlist = []
            if item [-4:]=='.dat':
                print(item)
                for line in county_file:
                    idx=0
                    line_list=[]
                    for i in widths:
                        increment=idx+i
                        line_list.append(line[idx:increment].strip())
                        idx=increment
                    parsedlist.append(line_list)
                for record in parsedlist:
                    temp_list = []
                    for item in record:
                        if item != '':
                            temp_list.append(item)
                    newlist.append(temp_list)

    
            elif item [-4:]=='.csv':
                print(item)
                headers = county_file.readline().strip().split(',')
                for line in county_file:
                    line = line.strip().split(',')
                    newlist.append(line)

            header_in = 'inflow' + in_years

            cur.execute("DROP TABLE IF EXISTS %s" %header_in)

            cur.execute("""
            CREATE TABLE %s (
            State_FIPS_Destination TEXT,
            County_FIPS_Destination TEXT,
            State_FIPS_Origin TEXT,
            County_FIPS_Origin TEXT,
            State_Abbrv_Origin TEXT,
            County_Origin TEXT,
            Number_Returns INTEGER,
            Number_Exemptions INTEGER,
            Adjusted_Gross_Income INTEGER)"""
            %header_in)         # Creates County in-flow data table

            cur.executemany('INSERT INTO %s VALUES(%s)'%(header_in,newmark), newlist)

            cur.execute('ALTER TABLE %s ADD COLUMN Unique_ID TEXT' %header_in)
            cur.execute('UPDATE %s SET Unique_ID = State_FIPS_Destination || County_FIPS_Destination || "_" || State_FIPS_Origin || County_FIPS_Origin' %header_in)

        print(cur.rowcount,'Records were written to the County in-flow table')
        con.commit()

    elif item [0:9]=='countyout':
        out_years = item[-8:-4]
        with open(item, 'rU') as county_out:
            parsedlist=[]
            newlist=[]
            if item [-4:]=='.dat':
                print(item)
                for line in county_out:
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
                            temp_list.append(item)
                    newlist.append(temp_list)

            elif item [-4:]== '.csv':
                print(item)
                headers = county_out.readline().strip().split(',')
                for line in county_out:
                    line = line.strip().split(',')
                    newlist.append(line)

            header_out = 'outflow' + out_years

            cur.execute("DROP TABLE IF EXISTS %s" %header_out)

            cur.execute("""
            CREATE TABLE %s (
            State_FIPS_Origin TEXT,
            County_FIPS_Origin TEXT,
            State_FIPS_Destination TEXT,
            County_FIPS_Destination TEXT,
            State_Abbrv_Destination TEXT,
            County_Destination TEXT,
            Number_Returns INTEGER,
            Number_Exemptions INTEGER,
            Adjusted_Gross_Income INTEGER)"""
            %header_out)        # Creates County out-flow data table

            cur.executemany('INSERT INTO %s VALUES(%s)' %(header_out,newmark), newlist)
            cur.execute('ALTER TABLE %s ADD COLUMN Unique_ID TEXT' %header_out)
            cur.execute('UPDATE %s SET Unique_ID = State_FIPS_Origin || County_FIPS_Origin || "_" || State_FIPS_Destination || County_FIPS_Destination' %header_out)

        print(cur.rowcount, 'Records were written to the County out-flow table')
        con.commit()

con.close()
