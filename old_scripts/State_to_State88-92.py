# This script was written to process the State-to-State IRS migration files from 1988 - 1992
# It loops through the inflow and outflow folders in States1988-1992, which contain .xls files
# for each state in each year recorded. Records are formatted into a list, and exported into an
# SQLite database. The path to the IRS data files, and the name of the SQLite database to be created
# must be hardcoded into the script before it is run. 

import xlrd, sqlite3, os

def sumdict(theval,thedict):
    if theval in thedict:
        thedict[theval]=thedict[theval]+1
    else:
        thedict[theval]=1

fieldnum = 7
qmark = '?,'*fieldnum
newmark = qmark[:-1]

con = sqlite3.connect('St88-92_test3-1-18.sqlite') #<- ENTER NAME OF DATABASE HERE
cur = con.cursor()

##This piece processes inflow records:##
##INSERT PATH TO DATA FILES##
from_cd = os.path.join('C:\\Users\\JBilladello\\Desktop\\irs_taxdata\\from_cd\\States1988-2004\\State1988-1992')
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
        Number_Exemptions INTEGER)""" 
        %header_in)
        
        for item in xls_files:
            workbook = xlrd.open_workbook(os.path.join(root, item))
            sheet = workbook.sheet_by_index(0)
            if item.endswith('89in.xls'):  # only for files in St8889in_excel
                print(item)
                for n in range(sheet.nrows)[8:61]:
                    state_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    del state_list[7]
                    del state_list[5]
                    del state_list[2] #deletes Percent Total Migrants columns and extra State Origin
                    state_list.insert(0, sheet.cell_value(3,3)) #only get State FIPS from cell
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap   #Capitalizes the second letter in the State Abbrv
                    state_fips = state_list[0][0:2]
                    state_list.insert(0, state_fips)
                    del state_list[1]
                    if state_list[2]=='' and state_list[3]=='TOTAL FLOW':   #Total Migration Flow
                        state_list.insert(3,sheet.cell_value(3,3)[3:5])
                        del state_list[2]
                        total = state_list[3].title()
                        del state_list[3]
                        state_list.insert(3,total)
                        string = str(int(state_list[1]))
                        del state_list[1]
                        state_list.insert(1,string)
                    elif state_list[2]=='' and state_list[3]=='State Non-Migrant':  #State Non-Migrant
                        state_list.insert(3,sheet.cell_value(3,3)[3:5])
                        del state_list[2]                    

                    unique_id = state_list[0] + '_' + str(state_list[1])
                    state_list.insert(0,unique_id)
                    print(state_list)

                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark), state_list)
                    con.commit()
                    
            elif item [-8:] != '89in.xls':      # for files that aren't 89in.xls
                for n in range(sheet.nrows)[8:61]:
                    state_list = []
                    caps_list = []
                    for i in range(sheet.ncols):                      
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    del state_list[6]
                    del state_list[4]
                    state_list.insert(0, sheet.cell_value(3,2)) # inserts TO: State into first column
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap   #Capitalizes the second letter in the State Abbrv
                    state_fips = state_list[0][0:2]
                    state_list.insert(0,state_fips)
                    del state_list[1]
                    if state_list[2]== '' and state_list[3]=='TOTAL FLOW':
                        state_list.insert(2,sheet.cell_value(3,2)[3:5])
                        del state_list[3]
                        total = state_list[3].title()
                        state_list.insert(3,total)
                        del state_list[4]
                        string = str(int(state_list[1]))
                        del state_list[1]
                        state_list.insert(1,string)
                    elif state_list[2]=='' and state_list[3]=='State Non-Migrant':
                        state_list.insert(2,sheet.cell_value(3,2)[3:5])
                        del state_list[3]
                    unique_id = state_list[0] + '_' + state_list[1]
                    state_list.insert(0, unique_id)
                    print(state_list)

                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark), state_list)
                    con.commit()
                    
##This piece processes the outflow records:##
##INSERT PATH TO DATA FILES##
from_cd_out = os.path.join('C:\\Users\\JBilladello\\Desktop\\irs_taxdata\\from_cd\\States1988-2004\\State1988-1992')                    
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
        Number_Exemptions INTEGER)"""
        %header_out)

        for item in xls_files:
            workbook = xlrd.open_workbook(os.path.join(root, item))
            sheet = workbook.sheet_by_index(0)
            if item.endswith('89out.xls'):  #only for files in St8889out_excel folder 
                print('FROM:',(sheet.cell_value(3,3)))
                for n in range(sheet.nrows)[8:61]:
                    state_list = []
                    caps_list = []
                    for i in range(sheet.ncols):                      
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    del state_list[7]
                    del state_list[5]
                    del state_list[2] #deletes Percent Total Migrants columns and extra State destination
                    state_list.insert(0, sheet.cell_value(3,3)) # inserts FROM: State into first column
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap   #Capitalizes the second letter in the State Abbrv
                    state_fips = state_list[0][0:2]
                    state_list.insert(0,state_fips)
                    del state_list[1]
                    if state_list[2]=='' and state_list[3]=='TOTAL FLOW':   #Total Migration Flow
                        state_list.insert(2,sheet.cell_value(3,3)[3:5])
                        del state_list[-4]
                        total = state_list[3].title()
                        del state_list[3]
                        state_list.insert(3,total)
                        string = str(int(state_list[1]))
                        state_list.insert(1,string)
                        del state_list[2]
                    elif state_list[2]=='' and state_list[3]=='State Non-Migrant':  #State Non-Migrant
                        state_list.insert(3,sheet.cell_value(3,3)[3:5])
                        del state_list[2]                    
                    unique_id = state_list[0] + '_' + state_list[1]
                    state_list.insert(0,unique_id)
                    print(state_list)

                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark), state_list)
                    con.commit()

            elif item [-9:] != '89out.xls':
                print(sheet.cell_value(0,0))
                print('FROM:', (sheet.cell_value(3,2)))
                for n in range(sheet.nrows)[8:61]:
                    state_list = []
                    caps_list = []
                    for i in range(sheet.ncols):
                        cellValue = (sheet.cell(n,i).value)
                        if isinstance(cellValue,str):
                            state_list.append(cellValue.strip())
                        else:
                            state_list.append(cellValue)
                    del state_list[6]
                    del state_list[4]
                    state_list.insert(0, sheet.cell_value(3,2))
                    state_cap = state_list[2].upper()
                    state_list[2] = state_cap
                    state_fips = state_list[0][0:2]
                    state_list.insert(0,state_fips)
                    del state_list[1]
                    if state_list[2]== '' and state_list[3]=='TOTAL FLOW':
                        state_list.insert(2,sheet.cell_value(3,2)[3:5])
                        del state_list[3]
                        total = state_list[3].title()
                        state_list.insert(3,total)
                        del state_list[4]
                        string = str(int(state_list[1]))
                        del state_list[1]
                        state_list.insert(1,string)
                    elif state_list[2]=='' and state_list[3]=='State Non-Migrant':
                        state_list.insert(2,sheet.cell_value(3,2)[3:5])
                        del state_list[3]

                    unique_id = state_list[0] + '_' + state_list[1]
                    state_list.insert(0,unique_id)
                    print(state_list)
                    
                    cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark), state_list)
                    con.commit()

con.close()
