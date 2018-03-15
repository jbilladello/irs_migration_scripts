# This script was written to process the State-to-State IRS migration files from 1992 - 2004
# It loops through the inflow and outflow folders in States1992-2004, which contain .xls files
# for each state in each year recorded. Records are formatted into a list, and exported to an
# SQLite databae. The path to the IRS data file, and the name of the SQLite database to be created
# must be hardcoded into the script before it is run

import xlrd, sqlite3, os

def sumdict(theval,thedict):
    if theval in thedict:
        thedict[theval]=thedict[theval]+1
    else:
        thedict[theval]=1

fieldnum = 8
qmark = '?,'*fieldnum
newmark = qmark[:-1]

con = sqlite3.connect('St92-04_test3-2-18.sqlite') #1992 - 2004
cur = con.cursor()

#inflow
from_cd = os.path.join('C:\\Users\\JBilladello\\Desktop\\irs_taxdata\\from_cd\\States1988-2004\\State1992-2004')
for root, dirs, files in os.walk(from_cd):
    xls_files = [ _ for _ in files if _.endswith('.xls')]
    i = 0
    n = 0
    if root.endswith('in_excel'):
        in_years = root[-12:-8]
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
        %header_in)

        for item in xls_files:
            workbook = xlrd.open_workbook(os.path.join(root, item))
            sheet = workbook.sheet_by_index(0)
            if item.endswith('93in.xls'):  #9293 in
                print(sheet.cell_value(0,0))
                print(sheet.cell_value(3,1))
                for n in range(sheet.nrows)[8:61]:
                    state_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    state_list.insert(0, sheet.cell_value(3,1))
                    if state_list[1] == "63":
                        state_list[1] = "96"
                    if state_list[2] == "XX":
                        state_list[2] = sheet.cell_value(60,1)
                    new_fips = state_list[0][0:2]
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap
                    state_list[0] = new_fips
                    unique_id = state_list[0] + '_' + state_list[1]
                    state_list.insert(0, unique_id)
                    print(state_list)

                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark), state_list)
                    con.commit()
                    
            elif item.endswith('94in.xls'):       #9394 in
                print(sheet.cell_value(0,0))
                print(sheet.cell_value(5,1))
                for n in range(sheet.nrows)[8:61]:
                    state_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    state_list.insert(0, sheet.cell_value(8,0))
                    if state_list[2] == '':
                        state_list[2] = sheet.cell_value(60,1)
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap
                    state_list[1] = str(state_list[1])
                    if len(state_list[1])== 3:
                        state_list[1] = '0'+ state_list[1]
                    state_list[1] = state_list[1][0:2]
                    new_fips = str(state_list[0])
                    if len(new_fips)==3:
                        state_list[0] = '0' + new_fips[0:1]
                    else:
                        state_list[0] = new_fips[0:2]
                    FIPS = state_list[0]
                    if state_list[1] == FIPS and state_list[3] == 'Total Inflow':
                        state_list[1] = '96'
                    unique_id = state_list[0] + '_' + state_list[1]
                    state_list.insert(0,unique_id)
                    print(state_list)
                    
                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark), state_list)
                    con.commit()

            elif item.endswith('95in.xls'):       # 9495 in
                print(sheet.cell_value(0,0))
                print(sheet.cell_value(3,1))
                for n in range(sheet.nrows)[8:61]:
                    state_list = []
                    caps_list =[]
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    state_list.insert(0, sheet.cell_value(8,0))
                    if state_list[2] == '':
                        state_list[2] = sheet.cell_value(60,1)
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap
                    state_name2 = state_list[3].title()
                    state_list[3] = state_name2
                    state_list[1] = str(state_list[1])
                    if len(state_list[1])== 3:
                        state_list[1] = '0' + state_list[1]
                    state_list[1] = state_list[1][0:2]
                    new_fips = str(state_list[0])
                    if len(new_fips)==3:
                        state_list[0] = '0' + new_fips[0:1]
                    else:
                        state_list[0] = new_fips[0:2]
                    FIPS = state_list[0]
                    if state_list[0] == FIPS and state_list[3] == 'Total Inflow':
                        state_list[1] = '96'
                    unique_id = state_list[0] + '_' + state_list[1]
                    state_list.insert(0,unique_id)
                    print(state_list)

                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark), state_list)
                    con.commit()

            elif item.startswith('s95') or item.endswith('97in.xls') or item.endswith('98in.xls') or item.endswith('99in.xls') or item.endswith('00in.xls'):   # 9596, 9697, 9798, 9899, 9900                                        
                print(sheet.cell_value(0,0))
                print(sheet.cell_value(3,1))
                for n in range(sheet.nrows)[8:63]:
                    state_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    state_list.insert(0, sheet.cell_value(3,1))
                    state_name = state_list[0][0:2]
                    state_list[0] = state_name
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap
                    state_name2 = state_list[3].title()
                    state_list[3] = state_name2
                    state_list[1] = str(state_list[1])
                    if len(state_list[1])== 3:
                        state_list[1] = '0'+ state_list[1]
                    state_list[1] = state_list[1][0:2]
                    unique_id = state_list[0] + '_' + state_list[1]
                    state_list.insert(0, unique_id)
                    print(state_list)
                    
                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark), state_list)
                    con.commit()

            elif item.endswith('01inr.xls') or item.endswith('02in.xls') or item.endswith('03in.xls') or item.endswith('04in.xls'):  # 0001, 0102, 0203, 0304   
                print(sheet.cell_value(3,0))
                for n in range(sheet.nrows)[8:63]:
                    state_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    state_list.insert(0, sheet.cell_value(3,0)) 
                    state_fips = state_list[0][4:6] 
                    state_list.insert(0, state_fips)
                    del state_list[1]
                    if state_list[0] == '':
                        state_list.insert(0, sheet.cell_value(3,1))
                        state_fips2 = state_list[0][0:2]
                        state_list.insert(0,state_fips2)
                        del state_list[1]
                        del state_list[1]
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap
                    origin = state_list[3].lower()
                    state_list[3] = origin.title()
                    unique_id = state_list[0] + '_' + state_list[1]
                    state_list.insert(0,unique_id)
                    print(state_list)
                    
                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark), state_list)
                    con.commit()
                    
#outflow
from_cd_out = os.path.join('C:\\Users\\JBilladello\\Desktop\\irs_taxdata\\from_cd\\States1988-2004\\State1992-2004')                    
for root, dirs, files in os.walk(from_cd_out):
    xls_files = [ _ for _ in files if _.endswith('.xls')]
    i = 0
    n = 0
    if root.endswith('out_excel'):
        out_years = root[-13:-9]
        header_out = 'outflow' + out_years

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
        %header_out)

        for item in xls_files:
            workbook = xlrd.open_workbook(os.path.join(root, item))
            sheet = workbook.sheet_by_index(0)
            if item.endswith('out.xls'):        # State 9293 out
                print(sheet.cell_value(0,0))
                print(sheet.cell_value(3,1))
                for n in range(sheet.nrows)[8:61]:
                    state_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    state_list.insert(0, sheet.cell_value(3,1))
                    if state_list[1] == "63":
                        state_list[1] = "96"
                    if state_list[2] == "XX":
                        state_list[2] = sheet.cell_value(60,1)
                    new_fips = state_list[0][0:2]
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap
                    state_list[0] = new_fips
                    unique_id = state_list[0] + '_' + state_list[1]
                    state_list.insert(0, unique_id)
                    print(state_list)
                    
                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark), state_list)
                    con.commit()
                    
            elif item.endswith('94ot.xls'):       #9394 out
                print(sheet.cell_value(0,0))
                print(sheet.cell_value(5,2))
                for n in range(sheet.nrows)[8:61]:
                    state_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    state_list.insert(0, sheet.cell_value(8,0))
                    if state_list[2] == '':
                        state_list[2] = sheet.cell_value(60,1)
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap
                    state_list[1] = str(state_list[1])
                    if len(state_list[1])== 3:
                        state_list[1] = '0'+ state_list[1]
                    state_list[1] = state_list[1][0:2]
                    new_fips = str(state_list[0])
                    if len(new_fips)==3:
                        state_list[0] = '0' + new_fips[0:1]
                    else:
                        state_list[0] = new_fips[0:2]
                    FIPS = state_list[0]
                    if state_list[1] == FIPS and state_list[3] == 'Total Outflow':
                        state_list[1] = '96'
                    unique_id = state_list[0] + '_' + state_list[1]
                    state_list.insert(0,unique_id)
                    print(state_list)
                    
                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark), state_list)
                    con.commit()
                
            elif item.endswith('95ot.xls'):       # 9495
                print(sheet.cell_value(0,0))
                print(sheet.cell_value(3,2))
                for n in range(sheet.nrows)[8:61]:
                    state_list = []
                    caps_list =[]
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    state_list.insert(0, sheet.cell_value(8,0))
                    if state_list[2] == '':
                        state_list[2] = sheet.cell_value(60,1)
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap
                    state_name2 = state_list[3].title()
                    state_list[3] = state_name2
                    state_list[1] = str(state_list[1])
                    if len(state_list[1])== 3:
                        state_list[1] = '0' + state_list[1]
                    state_list[1] = state_list[1][0:2]
                    new_fips = str(state_list[0])
                    if len(new_fips)==3:
                        state_list[0] = '0' + new_fips[0:1]
                    else:
                        state_list[0] = new_fips[0:2]
                    FIPS = state_list[0]
                    if state_list[0] == FIPS and state_list[3] == 'Total Outflow':
                        state_list[1] = '96'
                    unique_id = state_list[0] + '_' + state_list[1]
                    state_list.insert(0,unique_id)
                    print(state_list)

                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark), state_list)
                    con.commit()
                    

            elif item.startswith('s95') or item.endswith('97ot.xls') or item.endswith('98ot.xls') or item.endswith('99ot.xls') or item.endswith('00ot.xls'):   # 9596, 9697, 9798, 9899, 9900                                        
                print(sheet.cell_value(0,0))
                print(sheet.cell_value(3,2))
                for n in range(sheet.nrows)[8:63]:
                    state_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    state_list.insert(0, sheet.cell_value(3,2))
                    state_name = state_list[0][0:2]
                    state_list[0] = state_name
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap
                    state_name2 = state_list[3].title()
                    state_list[3] = state_name2
                    state_list[1] = str(state_list[1])
                    if len(state_list[1])== 3:
                        state_list[1] = '0'+ state_list[1]
                    state_list[1] = state_list[1][0:2]
                    unique_id = state_list[0] + '_' + state_list[1]
                    state_list.insert(0, unique_id)
                    print(state_list)
                    
                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark), state_list)
                    con.commit()

            elif item.endswith('01otr.xls') or item.endswith('02ot.xls') or item.endswith('03ot.xls') or item.endswith('04ot.xls'):  # 0001, 0102, 0203, 0304   
                print(sheet.cell_value(3,2))
                for n in range(sheet.nrows)[8:63]:
                    state_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    state_list.insert(0, sheet.cell_value(3,0)) 
                    state_fips = state_list[0][6:8]
                    state_list.insert(0, state_fips)
                    del state_list[1]
                    if state_list[0] == '':
                        state_list.insert(0, sheet.cell_value(3,2))
                        state_fips2 = state_list[0][0:2]
                        state_list.insert(0,state_fips2)
                        del state_list[1]
                        del state_list[1]
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap
                    origin = state_list[3].lower()
                    state_list[3] = origin.title()
                    unique_id = state_list[0] + '_' + state_list[1]
                    state_list.insert(0,unique_id)
                    print(state_list)
                    
                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark), state_list)
                    con.commit()

con.close()
