# This script is meant to process COUNTY 'inflow' IRS migration data from tax years 1990 - 1991 and 1991 - 1992
# Since the records for these years were reported in individual .txt files, grouped by state, the script loops through each state file,
# and parses the county records within into a SQLite database.
# A seperate script was written for 'outflow' county files for these years.

import os, csv, re, sqlite3

fieldnum = 8
qmark = '?,'*fieldnum
newmark = qmark[:-1]

con = sqlite3.connect('C:\\Users\\JBilladello\\Desktop\\irs_taxdata\\Final9092_DB\\test_9192in.sqlite')  # ENTER NAME OF SQLITE DATABASE TO BE CREATED (include years) 
cur = con.cursor()

county_data = os.path.join('C:\\Users\\JBilladello\\Desktop\\irs_taxdata\\from_cd\\Counties1990-2004\\1991-92 Migration Inflow') # ENTER PATH TO OUTFLOW DATA FOLDER (for years desired)

total_mig_state = "96"
total_mig_county = "000"

supp_st = '63' # state FIPS code assigned from 1992-1993 to 6 and 3-level suppressed values
state_abbrv = 'XX'
foreign_state = "57"

foreign_overseas = "001"
foreign_PR ="003"
foreign_apo = "005"
foreign_VI = "007"
foreign_abbrv = "FR"

in_years = 'inflow9192'
header_in = in_years
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
Number_Exemptions INTEGER)"""
%header_in)

for root, dirs, files in os.walk(county_data):
    txt_files = [ _ for _ in files if _.endswith('.txt')]
    for item in txt_files:
        readfile = open(os.path.join(root, item), 'r')
        print(item)
        
        all_list = []

        all_data = readfile.readlines()
        for line in all_data:
            all_lines = line.strip().split()
            all_list.append(all_lines)
        #print(len(all_list))

        out_rows = []
        result = []
        
        f = lambda x,y: x-y
      
        for loc,line in enumerate(all_data):
            if not line.startswith(' '):  #grab header lines
                state = line[0:2].strip()
                county = line[2:6].strip() #separate state and county fips and add to front
                header_line = line.strip().split()
                header_line.insert(2,total_mig_state)
                header_line.insert(3,total_mig_county)
                state_cap = header_line[-3].upper()
                del header_line[-3]
                header_line.insert(4,state_cap)
                header_join = ' '.join(map(str,header_line[5:-2]))
                del header_line[5:-2]
                header_line.insert(-2,header_join)
                #print(header_line)
                out_rows.append(header_line)
                    
            elif line.startswith(' '):
                sub_group = line #grab sub-counties
                two_digits = re.compile(r'\d\d').search(sub_group).group()
                if sub_group[2:4] == two_digits and 'Foreign /' not in sub_group:
                    sub_counties = sub_group.strip().split()
                    sub_counties.insert(0,state)
                    sub_counties.insert(1,county)
                    del sub_counties[-1]
                    del sub_counties[-2]
                    state_cap = sub_counties[-3].upper()
                    del sub_counties[-3]
                    sub_counties.insert(4,state_cap)
                    counties_join = ' '.join(map(str, sub_counties[5:-2]))
                    del sub_counties[5:-2]
                    sub_counties.insert(-2, counties_join)
                    #print(sub_counties)
                    out_rows.append(sub_counties)

                if 'Foreign /' in sub_group:
                    foreign_sub = sub_group.strip().split()
                    foreign_sub.insert(0,state)
                    foreign_sub.insert(1,county)
                    del foreign_sub[-1]
                    del foreign_sub[-2]
                    foreign_join = ' '.join(map(str,foreign_sub[4:-3]))
                    del foreign_sub[4:-3]
                    foreign_sub.insert(-2,foreign_join)
                    #print(foreign_sub)
                    out_rows.append(foreign_sub)

                elif 'Foreign' in sub_group and '/' not in sub_group: #deals with suppressed foreign designation
                    foreign = "015"
                    foreign_supp = sub_group.strip().split()
                    foreign_supp.insert(0,state)
                    foreign_supp.insert(1,county)
                    foreign_supp.insert(2,supp_st)
                    foreign_supp.insert(3,foreign)
                    del foreign_supp[-1]
                    del foreign_supp[-2]
                    foreign_supp.insert(4,state_abbrv)
                    #print(foreign_supp)
                    out_rows.append(foreign_supp)

                if 'County Non-Migrants' in sub_group:   #County Non-Migrants
                    non_mig = sub_group.strip().split()
                    non_mig.insert(0,state)
                    non_mig.insert(1,county)
                    non_mig_join = ' '.join(map(str,non_mig[4:6]))
                    del non_mig[4:6]
                    non_mig.insert(-2,non_mig_join)
                    non_mig.insert(4,state_cap)
                    #print(non_mig)
                    out_rows.append(non_mig)

                if 'All Migration' in sub_group:  #All Migration Flows
                    suppress_all = '030'
                    all_mig = sub_group.strip().split()
                    all_mig.insert(0,state)
                    all_mig.insert(1,county)
                    all_mig.insert(2,supp_st)
                    all_mig.insert(3,suppress_all)
                    del all_mig[-1]
                    del all_mig[-2]
                    all_mig_join = ' '.join(map(str,all_mig[4:7]))
                    del all_mig[4:7]
                    all_mig.insert(-2,all_mig_join)
                    all_mig.insert(4,state_abbrv)
                    #print(all_mig)
                    out_rows.append(all_mig)
################################################################################# three-level suppression and variants
                    
                if 'Same State' in sub_group and 'Same Region' in all_data[loc+1]:
                    same_st_three = '020'
                    same_reg_diff_state = "021"
                    diff_reg = "022"
                    three_level = sub_group.strip().split()
                    three_level.insert(0,state)
                    three_level.insert(1,county)
                    three_level.insert(2,supp_st)
                    three_level.insert(3,same_st_three)
                    del three_level[-1]
                    del three_level[-2]
                    three_level_join = ' '.join(map(str,three_level[4:6]))
                    del three_level[4:6]
                    three_level.insert(-2,three_level_join)
                    three_level.insert(4,state_abbrv)
                    #print(three_level)
                    out_rows.append(three_level)
                    
                    different_st = all_data[loc+1]
                    diff_state = different_st.strip().split()
                    diff_state.insert(0,state)
                    diff_state.insert(1,county)
                    diff_state.insert(2,supp_st)
                    diff_state.insert(3,same_reg_diff_state)
                    del diff_state[-1]
                    del diff_state[-2]
                    diff_state_join = ' '.join(map(str,diff_state[4:8]))
                    del diff_state[4:8]
                    diff_state.insert(-2,diff_state_join)
                    diff_state.insert(4,state_abbrv)
                    #print(diff_state)
                    out_rows.append(diff_state)

                    different_reg = all_data[loc+2]
                    diff_region = different_reg.strip().split()
                    diff_region.insert(0,state)
                    diff_region.insert(1,county)
                    diff_region.insert(2,supp_st)
                    diff_region.insert(3,diff_reg)
                    del diff_region[-1]
                    del diff_region[-2]
                    diff_reg_join = ' '.join(map(str,diff_region[4:6]))
                    del diff_region[4:6]
                    diff_region.insert(-2,diff_reg_join)
                    diff_region.insert(4,state_abbrv)
                    #print(diff_region)
                    out_rows.append(diff_region)
############################################################################### six-level suppression and variants

                elif 'Same State' in sub_group and 'Same Region' not in all_data[loc+1]:
                    same_st_six = "010"
                    six_level = sub_group.strip().split()
                    six_level.insert(0,state)
                    six_level.insert(1,county)
                    six_level.insert(2,supp_st)
                    six_level.insert(3,same_st_six)
                    del six_level[-1]
                    del six_level[-2]
                    six_lev_join = ' '.join(map(str,six_level[4:6]))
                    del six_level[4:6]
                    six_level.insert(-2,six_lev_join)
                    six_level.insert(4,state_abbrv)
                    #print(header_line)
                    #print(six_level)
                    out_rows.append(six_level)                  

                if 'Region 1' in sub_group:
                    reg1 = "011"
                    region_1 = sub_group
                    one = region_1.strip().split()
                    one.insert(0,state)
                    one.insert(1,county)
                    one.insert(2,supp_st)
                    one.insert(3,reg1)
                    del one[-1]
                    del one[-2]
                    one.insert(4,state_abbrv)
                    one_join = ' '.join(map(str,one[-5:-2]))
                    del one[-5:-2]
                    one.insert(-2,one_join)
                    #print(one)
                    out_rows.append(one)

                if 'Region 2' in sub_group:
                    reg2 = "012"
                    region_2 = sub_group
                    two = region_2.strip().split()
                    two.insert(0,state)
                    two.insert(1,county)
                    two.insert(2,supp_st)
                    two.insert(3,reg2)
                    del two[-1]
                    del two[-2]
                    two.insert(4,state_abbrv)
                    two_join = ' '.join(map(str,two[-5:-2]))
                    del two[-5:-2]
                    two.insert(-2,two_join)
                    #print(two)
                    out_rows.append(two)

                if 'Region 3' in sub_group:
                    reg3 = "013"
                    region_3 = sub_group
                    three = region_3.strip().split()
                    three.insert(0,state)
                    three.insert(1,county)
                    three.insert(2,supp_st)
                    three.insert(3,reg3)
                    del three[-1]
                    del three[-2]
                    three.insert(4,state_abbrv)
                    three_join = ' '.join(map(str,three[-5:-2]))
                    del three[-5:-2]
                    three.insert(-2,three_join)
                    #print(three)
                    out_rows.append(three)

                if 'Region 4' in sub_group:
                    reg4 = "014"
                    region_4 = sub_group
                    four = region_4.strip().split()
                    four.insert(0,state)
                    four.insert(1,county)
                    four.insert(2,supp_st)
                    four.insert(3,reg4)
                    del four[-1]
                    del four[-2]
                    four.insert(4,state_abbrv)
                    four_join = ' '.join(map(str,four[-5:-2]))
                    del four[-5:-2]
                    four.insert(-2,four_join)
                    #print(four)
                    out_rows.append(four)
                    
        #print(len(out_rows))                    

        result = f(len(all_list),len(out_rows))

        #print(result)
        if result != 0:
            print(item)
            print(f(len(all_list),len(out_rows)))

        for objects in out_rows:
            cur.execute('INSERT INTO %s VALUES(%s)' %(header_in,newmark),objects)
            con.commit()

            
cur.execute('ALTER TABLE %s ADD COLUMN Unique_ID TEXT' %header_in)
cur.execute('UPDATE %s SET Unique_ID = State_FIPS_Destination || County_FIPS_Destination || "_" || State_FIPS_Origin || County_FIPS_Origin' %header_in)
con.commit()

con.close()

print ('done')
