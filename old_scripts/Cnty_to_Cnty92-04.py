# Loop through Counties1990-2004 folder (only imports one table at a time, need to re-run?)

import csv, sqlite3, os, xlrd

def sumdict(theval,thedict):
    if theval in thedict:
        thedict[theval]=thedict[theval]+1
    else:
        thedict[theval]=1

fieldnum = 10
qmark = '?,'*fieldnum
newmark = qmark[:-1]

con = sqlite3.connect('C:\\Users\\jbilladello.BC\\Desktop\\Copy_IRS_db\\test_dbs\\test_county_in.sqlite')
cur = con.cursor()

#inflow
from_cd = os.path.join('C:\\Users\\JBilladello\\Desktop\\irs_taxdata\\from_cd\\Counties1990-2004')
for root, dirs, files in os.walk(from_cd):
    xls_files = [ _ for _ in files if _.endswith('.xls')] #deals with just the xls files, need to add .txt
    i = 0
    n = 0
    if root.endswith('Inflow'):
        id_codes = {}
        for item in xls_files:
            if item[0] == 'C':
                in_years = item[1:5]
            elif item[0:4] == 'co93' or item[0:4] == 'co94' or item[0:4] == 'co95' or item[0:4] == 'co96' or item[0:4] == 'co97' or item[0:4] == 'co98':
                in_years = item[2:4] + '9' + item[4]
            elif item[0:4] == 'co99':
                in_years = item[2:4] + '0' + item[4]
            elif item[3] == '0' or item[2] == '1' or item[2] == '2':
                in_years = '0' + item[2:5]
            elif item.startswith('co03'):
                in_years = item[2:6]
                
            header_in = 'inflow' + in_years
            cur.execute("DROP TABLE IF EXISTS %s" %header_in)
            
            cur.execute("""
            CREATE TABLE %s (
            Unique_ID TEXT PRIMARY KEY,                
            State_FIPS_Destination TEXT,
            County_FIPS_Destination TEXT,
            State_FIPS_Origin TEXT,
            County_FIPS_Origin TEXT,
            State_Abbrv_Origin TEXT,
            County_Origin TEXT,
            Number_Returns INTEGER,
            Number_Exemptions INTEGER,
            Adjusted_Gross_Income INTEGER)""" 
            %header_in)
            
        for item in xls_files: 
            workbook = xlrd.open_workbook(os.path.join(root, item))
            sheet = workbook.sheet_by_index(0)
            
            if item.startswith('C92'):   #9293
                print('TO:', (sheet.cell_value(5,0)))
                for n in range(sheet.nrows)[8:]: #end number varies by state *** xlrd.empty_cell.value
                    county_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue, str):
                            county_list.append(cellValue.strip())
                        else:
                            county_list.append(cellValue)
                    state_cap = county_list[4].upper() # converts the State Abbrv to caps
                    county_list[4] = state_cap
                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
                    county_list.insert(0,unique_id)                
                    sumdict(unique_id,id_codes)
                    if id_codes.get(unique_id)>1:
                        print('unique id is duplicated!',unique_id)
                        raise SystemExit
                    else:
                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark),county_list)                    

            elif item.startswith('co93'):  #9394
                print('TO:', (sheet.cell_value(5,0)))
                for n in range(sheet.nrows)[8:]: 
                    county_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue, str):
                            county_list.append(cellValue.strip())
                        else:
                            county_list.append(cellValue)
                    state_cap = county_list[4].upper() # converts the State Abbrv to caps
                    county_list[4] = state_cap
                    county_list[0] = str(county_list[0]) # converts the State FIPS from decimals
                    if len(county_list[0]) <= 3: 
                        county_list[0] = '0' + county_list[0]
                    county_list[0] = county_list[0][0:2]

                    county_list[2] = str(county_list[2])
                    if len(county_list[2]) <= 3:
                        county_list[2] = '0' + county_list[2]
                    county_list[2] = county_list[2][0:2]

                    county_list[1] = str(county_list[1]) # converts the County FIPS from decimals
                    if len(county_list[1]) <= 4:
                        county_list[1] = '0' + county_list[1][0:2]
                    elif len(county_list[1]) > 4:
                        county_list[1] = county_list[1][0:3]
                    if county_list[1].endswith('.'):
                        county_list[1] = '00' + county_list[1][1]

                    county_list[3] = str(county_list[3])
                    if len(county_list[3]) <= 4:
                        county_list[3] = '0' + county_list[3][0:2]
                    elif len(county_list[3]) > 4:
                        county_list[3] = county_list[3][0:3]
                    if county_list[3].endswith('.'):
                        county_list[3] = '00' + county_list[3][1]
                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]        
                    county_list.insert(0,unique_id)
                    sumdict(unique_id,id_codes)
                    if id_codes.get(unique_id)>1:
                        print('unique id is duplicated!',unique_id)
                        raise SystemExit
                    else:
                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark),county_list)                    
 
            elif item.startswith('co94'):  #9495
                print('TO:', (sheet.cell_value(4,0)))
                for n in range(sheet.nrows)[8:]: 
                    county_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue, str):
                            county_list.append(cellValue.strip())
                        else:
                            county_list.append(cellValue)
                    state_cap = county_list[4].upper() # converts the State Abbrv to caps
                    county_list[4] = state_cap
                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
                    county_list.insert(0,unique_id)
                    sumdict(unique_id,id_codes)
                    if id_codes.get(unique_id)>1:
                        print('unique id is duplicated!',unique_id)
                        raise SystemExit
                    else:
                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark),county_list)

            elif item.startswith('co95') and item != 'co956usir.xls':  #9596
                print('TO:', (sheet.cell_value(4,0)))
                for n in range(sheet.nrows)[8:]: 
                    county_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue, str):
                            county_list.append(cellValue.strip())
                        else:
                            county_list.append(cellValue)
                    state_cap = county_list[4].upper() # converts the State Abbrv to caps
                    county_list[4] = state_cap
                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
                    county_list.insert(0,unique_id)
                    sumdict(unique_id,id_codes)
                    if id_codes.get(unique_id)>1:
                        print('unique id is duplicated!',unique_id)
                        raise SystemExit
                    else:
                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark),county_list)
                  
            elif item.startswith('co96') and item != 'co967usi.xls': #9697
                print('TO:', (sheet.cell_value(4,0)))
                for n in range(sheet.nrows)[8:]: 
                    county_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue, str):
                            county_list.append(cellValue.strip())
                        else:
                            county_list.append(cellValue)
                    state_cap = county_list[4].upper() # converts the State Abbrv to caps
                    county_list[4] = state_cap
                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3] #creates unique_id column
                    county_list.insert(0,unique_id)
                    sumdict(unique_id,id_codes)  #calls function
                    if id_codes.get(unique_id)>1:
                        print('unique id is duplicated!',unique_id)
                        raise SystemExit
                    else:
                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark),county_list)

            elif item.startswith('co97')and item != 'co978usi.xls':  #9798
                print('TO:', (sheet.cell_value(4,0)))
                for n in range(sheet.nrows)[8:]:
                    county_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue, str):
                            county_list.append(cellValue.strip())
                        else:
                            county_list.append(cellValue)
                    state_cap = county_list[4].upper()
                    county_list[4] = state_cap
                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3] #creates unique_id column
                    county_list.insert(0,unique_id)
                    sumdict(unique_id,id_codes)  #calls function
                    if id_codes.get(unique_id)>1:
                        print('unique id is duplicated!',unique_id)
                        raise SystemExit
                    else:
                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark),county_list)                    

            elif item.startswith('co98') and item != 'co989usi.xls':  #9899
                print('TO:', (sheet.cell_value(5,0)))
                for n in range(sheet.nrows)[8:]:
                    county_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue, str):
                            county_list.append(cellValue.strip())
                        else:
                            county_list.append(cellValue)
                    state_cap = county_list[4].upper() 
                    county_list[4] = state_cap
                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
                    county_list.insert(0,unique_id)
                    sumdict(unique_id,id_codes) 
                    if id_codes.get(unique_id)>1:
                        print('unique id is duplicated!',unique_id)
                        raise SystemExit
                    else:
                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark),county_list)
                    
            elif item.startswith('co99') and item != 'co990usi.xls':  #9900
                print('TO:', (sheet.cell_value(5,0)))
                for n in range(sheet.nrows)[8:]:
                    county_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue, str):
                            county_list.append(cellValue.strip())
                        else:
                            county_list.append(cellValue)
                    state_cap = county_list[4].upper()
                    county_list[4] = state_cap
                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
                    county_list.insert(0,unique_id)
                    sumdict(unique_id,id_codes) 
                    if id_codes.get(unique_id)>1:
                        print('unique id is duplicated!',unique_id)
                        raise SystemExit
                    else:
                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark),county_list) 
                    
            elif item.startswith('co00') and item != 'co001usir.xls' or item.startswith('co10') and item != 'co102usi.xls':  #9900, 0001, 0102
                print('TO:', (sheet.cell_value(5,0)))
                for n in range(sheet.nrows)[8:]:
                    county_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue, str):
                            county_list.append(cellValue.strip())
                        else:
                            county_list.append(cellValue)
                    state_cap = county_list[4].upper()
                    county_list[4] = state_cap
                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
                    county_list.insert(0,unique_id)
                    sumdict(unique_id,id_codes) 
                    if id_codes.get(unique_id)>1:
                        print('unique id is duplicated!',unique_id)
                        raise SystemExit
                    else:
                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark),county_list)

            elif item.startswith('co20'):
                print('TO:', (sheet.cell_value(4,0)))
                for n in range(sheet.nrows)[8:]:
                    county_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue, str):
                            county_list.append(cellValue.strip())
                        else:
                            county_list.append(cellValue)
                    state_cap = county_list[4].upper()
                    county_list[4] = state_cap
                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
                    county_list.insert(0,unique_id)
                    sumdict(unique_id,id_codes) 
                    if id_codes.get(unique_id)>1:
                        print('unique id is duplicated!',unique_id)
                        raise SystemExit
                    else:
                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark),county_list)
            
            elif item.startswith('co03'):
                print('TO:', (sheet.cell_value(5,0)))
                for n in range(sheet.nrows)[8:]:
                    county_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue, str):
                            county_list.append(cellValue.strip())
                        else:
                            county_list.append(cellValue)
                    state_cap = county_list[4].upper()
                    county_list[4] = state_cap
                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
                    county_list.insert(0,unique_id)
                    sumdict(unique_id,id_codes) 
                    if id_codes.get(unique_id)>1:
                        print('unique id is duplicated!',unique_id)
                        raise SystemExit
                    else:
                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark),county_list)
                            
            else:
                pass


###outflow
##from_cd = os.path.join('C:\\Users\\jbilladello.BC\\Desktop\\Copy_IRS_db\\irs_taxdata\\from_cd\\Counties1990-2004')
##for root, dirs, files in os.walk(from_cd):
##    xls_files = [ _ for _ in files if _.endswith('.xls')] #deals with just the xls files, need to add .txt
##    i = 0
##    n = 0
##    if root.endswith('Outflow'):
##        id_codes = {}
##        for item in xls_files:
##            if item[0] == 'C':
##                out_years = item[1:5]
##            elif item[0:4] == 'co93' or item[0:4] == 'co94' or item[0:4] == 'co95' or item[0:4] == 'co97' or item[0:4] == 'co98':
##                out_years = item[2:4] + '9' + item[4]
##            elif item[3] == '6':
##                out_years = item[2:4] + '9' + item[4]           # CO in script
##            elif item[0:4] == 'co99':
##                out_years = item[2:4] + '0' + item[4]
##            elif item[3] == '0' or item[2] == '1' or item[2] == '2':
##                out_years = '0' + item[2:5]
##            elif item.startswith('co0304'):
##                out_years = item[2:6]
##
##            header_out = 'outflow' + out_years
##            cur.execute("DROP TABLE IF EXISTS %s" %header_out)
##            
##            cur.execute("""
##            CREATE TABLE %s (
##            Unique_ID TEXT PRIMARY KEY,
##            State_FIPS_Origin TEXT,
##            County_FIPS_Origin TEXT,
##            State_FIPS_Destination TEXT,
##            County_FIPS_Destination TEXT,
##            State_Abbrv_Destination TEXT,
##            County_Destination TEXT,
##            Number_Returns INTEGER,
##            Number_Exemptions INTEGER,
##            Adjusted_Gross_Income INTEGER)"""
##            %header_out)
##
##        for item in xls_files: 
##            workbook = xlrd.open_workbook(os.path.join(root, item))
##            sheet = workbook.sheet_by_index(0)
##            
##            if item.startswith('C92'):   #9293
##                print('TO:', (sheet.cell_value(5,0)))
##                for n in range(sheet.nrows)[8:]:
##                    county_list = []
##                    caps_list = []
##                    for i in range(sheet.ncols):
##                        cellValue = (sheet.cell(n,i).value)
##                        if isinstance(cellValue, str):
##                            county_list.append(cellValue.strip())
##                        else:
##                            county_list.append(cellValue)
##                    state_cap = county_list[4].upper() # converts the State Abbrv to caps
##                    county_list[4] = state_cap
##                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
##                    county_list.insert(0,unique_id)                
##                    sumdict(unique_id,id_codes)
##                    if id_codes.get(unique_id)>1:
##                        print('unique id is duplicated!',unique_id)
##                        raise SystemExit
##                    else:
##                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark),county_list)
##                        
##            elif item.startswith('co93'):  #9394
##                print('TO:', (sheet.cell_value(5,0)))
##                for n in range(sheet.nrows)[8:]: 
##                    county_list = []
##                    caps_list = []
##                    for i in range(sheet.ncols):
##                        cellValue = (sheet.cell(n,i).value)
##                        if isinstance(cellValue, str):
##                            county_list.append(cellValue.strip())
##                        else:
##                            county_list.append(cellValue)
##                    state_cap = county_list[4].upper() # converts the State Abbrv to caps
##                    county_list[4] = state_cap
##                    county_list[0] = str(county_list[0]) # converts the State FIPS from decimals
##                    if len(county_list[0]) <= 3: 
##                        county_list[0] = '0' + county_list[0]
##                    county_list[0] = county_list[0][0:2]
##
##                    county_list[2] = str(county_list[2])
##                    if len(county_list[2]) <= 3:
##                        county_list[2] = '0' + county_list[2]
##                    county_list[2] = county_list[2][0:2]
##
##                    county_list[1] = str(county_list[1]) # converts the County FIPS from decimals
##                    if len(county_list[1]) <= 4:
##                        county_list[1] = '0' + county_list[1][0:2]
##                    elif len(county_list[1]) > 4:
##                        county_list[1] = county_list[1][0:3]
##                    if county_list[1].endswith('.'):
##                        county_list[1] = '00' + county_list[1][1]
##
##                    county_list[3] = str(county_list[3])
##                    if len(county_list[3]) <= 4:
##                        county_list[3] = '0' + county_list[3][0:2]
##                    elif len(county_list[3]) > 4:
##                        county_list[3] = county_list[3][0:3]
##                    if county_list[3].endswith('.'):
##                        county_list[3] = '00' + county_list[3][1]
##                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]        
##                    county_list.insert(0,unique_id)
##                    sumdict(unique_id,id_codes)
##                    if id_codes.get(unique_id)>1:
##                        print('unique id is duplicated!',unique_id)
##                        raise SystemExit
##                    else:
##                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark),county_list)
##                        
##            elif item.startswith('co94'):  #9495
##                print('TO:', (sheet.cell_value(4,0)))
##                for n in range(sheet.nrows)[8:]: 
##                    county_list = []
##                    caps_list = []
##                    for i in range(sheet.ncols):
##                        cellValue = (sheet.cell(n,i).value)
##                        if isinstance(cellValue, str):
##                            county_list.append(cellValue.strip())
##                        else:
##                            county_list.append(cellValue)
##                    state_cap = county_list[4].upper() # converts the State Abbrv to caps
##                    county_list[4] = state_cap
##                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
##                    county_list.insert(0,unique_id)
##                    sumdict(unique_id,id_codes)
##                    if id_codes.get(unique_id)>1:
##                        print('unique id is duplicated!',unique_id)
##                        raise SystemExit
##                    else:
##                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark),county_list)
##
##            elif item.startswith('co95') and item != 'co956usor.xls':  #9596
##                print('TO:', (sheet.cell_value(4,0)))
##                for n in range(sheet.nrows)[8:]: 
##                    county_list = []
##                    caps_list = []
##                    for i in range(sheet.ncols):
##                        cellValue = (sheet.cell(n,i).value)
##                        if isinstance(cellValue, str):
##                            county_list.append(cellValue.strip())
##                        else:
##                            county_list.append(cellValue)
##                    state_cap = county_list[4].upper() # converts the State Abbrv to caps
##                    county_list[4] = state_cap
##                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
##                    county_list.insert(0,unique_id)
##                    sumdict(unique_id,id_codes)
##                    if id_codes.get(unique_id)>1:
##                        print('unique id is duplicated!',unique_id)
##                        raise SystemExit
##                    else:
##                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark),county_list)
##
##            elif item.startswith('co96') and item != 'co967usi.xls': #9697
##                print('TO:', (sheet.cell_value(4,0)))
##                for n in range(sheet.nrows)[8:]: 
##                    county_list = []
##                    caps_list = []
##                    for i in range(sheet.ncols):
##                        cellValue = (sheet.cell(n,i).value)
##                        if isinstance(cellValue, str):
##                            county_list.append(cellValue.strip())
##                        else:
##                            county_list.append(cellValue)
##                    state_cap = county_list[4].upper() # converts the State Abbrv to caps
##                    county_list[4] = state_cap
##                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3] #creates unique_id column
##                    county_list.insert(0,unique_id)
##                    sumdict(unique_id,id_codes)  #calls function
##                    if id_codes.get(unique_id)>1:
##                        print('unique id is duplicated!',unique_id)
##                        raise SystemExit
##                    else:
##                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark),county_list)
##
##            elif item.startswith('co97')and item != 'co978usi.xls':  #9798
##                print('TO:', (sheet.cell_value(4,0)))
##                for n in range(sheet.nrows)[8:]:
##                    county_list = []
##                    caps_list = []
##                    for i in range(sheet.ncols):
##                        cellValue = (sheet.cell(n,i).value)
##                        if isinstance(cellValue, str):
##                            county_list.append(cellValue.strip())
##                        else:
##                            county_list.append(cellValue)
##                    state_cap = county_list[4].upper()
##                    county_list[4] = state_cap
##                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3] #creates unique_id column
##                    county_list.insert(0,unique_id)
##                    sumdict(unique_id,id_codes)  #calls function
##                    if id_codes.get(unique_id)>1:
##                        print('unique id is duplicated!',unique_id)
##                        raise SystemExit
##                    else:
##                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark),county_list)
##
##            elif item.startswith('co98') and item != 'co989uso.xls':  #9899
##                print('TO:', (sheet.cell_value(5,0)))
##                for n in range(sheet.nrows)[8:]:
##                    county_list = []
##                    caps_list = []
##                    for i in range(sheet.ncols):
##                        cellValue = (sheet.cell(n,i).value)
##                        if isinstance(cellValue, str):
##                            county_list.append(cellValue.strip())
##                        else:
##                            county_list.append(cellValue)
##                    state_cap = county_list[4].upper() 
##                    county_list[4] = state_cap
##                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
##                    county_list.insert(0,unique_id)
##                    sumdict(unique_id,id_codes) 
##                    if id_codes.get(unique_id)>1:
##                        print('unique id is duplicated!',unique_id)
##                        raise SystemExit
##                    else:
##                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark),county_list)
##                    
##            elif item.startswith('co99') and item != 'co990uso.xls':  #9900
##                print('TO:', (sheet.cell_value(5,0)))
##                for n in range(sheet.nrows)[8:]:
##                    county_list = []
##                    caps_list = []
##                    for i in range(sheet.ncols):
##                        cellValue = (sheet.cell(n,i).value)
##                        if isinstance(cellValue, str):
##                            county_list.append(cellValue.strip())
##                        else:
##                            county_list.append(cellValue)
##                    state_cap = county_list[4].upper()
##                    county_list[4] = state_cap
##                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
##                    county_list.insert(0,unique_id)
##                    sumdict(unique_id,id_codes) 
##                    if id_codes.get(unique_id)>1:
##                        print('unique id is duplicated!',unique_id)
##                        raise SystemExit
##                    else:
##                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark),county_list)
##
##            elif item.startswith('co00') and item != 'co001usor.xls' or item.startswith('co10') and item != 'co102uso.xls':  #0001, 0102
##                print('TO:', (sheet.cell_value(5,0)))
##                for n in range(sheet.nrows)[8:]:
##                    county_list = []
##                    caps_list = []
##                    for i in range(sheet.ncols):
##                        cellValue = (sheet.cell(n,i).value)
##                        if isinstance(cellValue, str):
##                            county_list.append(cellValue.strip())
##                        else:
##                            county_list.append(cellValue)
##                    state_cap = county_list[4].upper()
##                    county_list[4] = state_cap
##                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
##                    county_list.insert(0,unique_id)
##                    sumdict(unique_id,id_codes) 
##                    if id_codes.get(unique_id)>1:
##                        print('unique id is duplicated!',unique_id)
##                        raise SystemExit
##                    else:
##                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark),county_list)
##
##            elif item.startswith('co20') and item != 'co203uso.xls':
##                print('TO:', (sheet.cell_value(4,0)))
##                for n in range(sheet.nrows)[8:]:
##                    county_list = []
##                    caps_list = []
##                    for i in range(sheet.ncols):
##                        cellValue = (sheet.cell(n,i).value)
##                        if isinstance(cellValue, str):
##                            county_list.append(cellValue.strip())
##                        else:
##                            county_list.append(cellValue)
##                    state_cap = county_list[4].upper()
##                    county_list[4] = state_cap
##                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
##                    county_list.insert(0,unique_id)
##                    sumdict(unique_id,id_codes) 
##                    if id_codes.get(unique_id)>1:
##                        print('unique id is duplicated!',unique_id)
##                        raise SystemExit
##                    else:
##                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark),county_list)
##            
##            elif item.startswith('co03'):
##                print('TO:', (sheet.cell_value(5,0)))
##                for n in range(sheet.nrows)[8:]:
##                    county_list = []
##                    caps_list = []
##                    for i in range(sheet.ncols):
##                        cellValue = (sheet.cell(n,i).value)
##                        if isinstance(cellValue, str):
##                            county_list.append(cellValue.strip())
##                        else:
##                            county_list.append(cellValue)
##                    state_cap = county_list[4].upper()
##                    county_list[4] = state_cap
##                    unique_id = county_list[0] + county_list[1] + '_' + county_list[2] + county_list[3]
##                    county_list.insert(0,unique_id)
##                    sumdict(unique_id,id_codes) 
##                    if id_codes.get(unique_id)>1:
##                        print('unique id is duplicated!',unique_id)
##                        raise SystemExit
##                    else:
##                        cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark),county_list)
##                            
##            else:
##                pass
##

con.commit()
con.close()


