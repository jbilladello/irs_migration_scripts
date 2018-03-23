# This script is meant to process COUNTY 'outflow' IRS migration data from tax years 1990 - 1991 and 1991 - 1992
# Since the records for these years were reported in individual .txt files, grouped by state, the script loops through each state file,
# and parses the county records within into a SQLite database.
# The name of the SQLite database to be created, and the path to the folder that contains the outflow data files must be hardcoded at the top
# before running the script.
# A seperate script was written for 'inflow' county files for these years.

import os, csv, re, sqlite3

fieldnum = 8
qmark = '?,'*fieldnum
newmark = qmark[:-1]

con = sqlite3.connect('C:\\Users\\JBilladello\\Desktop\\irs_taxdata\\Final9092_DB\\test_9192out.sqlite') # ENTER NAME OF SQLITE DATABASE TO BE CREATED (include years)
cur = con.cursor()

county_data = os.path.join('C:\\Users\\JBilladello\\Desktop\\irs_taxdata\\from_cd\\Counties1990-2004\\1991-92 Migration Outflow') # ENTER PATH TO OUTFLOW DATA FOLDER (for years desired)

total_mig_state = "96"
total_mig_county = "000"

supp_st = '63' #state FIPS code assigned from 1992-1993 to 6 and 3-level suppressed values
state_abbrv = 'XX'
foreign_state = "57"

foreign_overseas = "001"
foreign_PR ="003"
foreign_apo = "005"
foreign_VI = "007"
foreign_abbrv = "FR"

out_years = 'outflow9192'
header_out = out_years
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
Number_Exemptions INTEGER)"""
%header_out)

for root, dirs, files in os.walk(county_data):
    txt_files = [ _ for _ in files if _.endswith('.txt')]
    for item in txt_files:
        readfile = open(os.path.join(root, item), 'r')
        print(item)

        all_data = readfile.readlines()
        #print(len(all_data))

        all_data_list = open('all_list9192out.csv', "a")    ### EXPORT AS TABLE TO COMPARE WITH OUT_ROWS
        for item in all_data:
            line = item.strip().split()
            all_data_list.writelines(",".join(line)+"\n")

        out_rows = []

        f = lambda x,y: x-y            ##subtracts values in all_data from exported values in out_rows
      
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
                
                if 'Same State' in sub_group and 'Region 1' in all_data[loc+1]:
                    same_st_six = "010"
                    reg1 = "011"
                    reg2 = "012"
                    reg3 = "013"
                    reg4 = "014"
                    foreign = "015"
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
                    #print(six_level)
                    out_rows.append(six_level)

                    region_1 = all_data[loc+1]
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

                    region_2 = all_data[loc+2]
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

                    region_3 = all_data[loc+3]
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

                    region_4 = all_data[loc+4]
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
                    
                    if 'Same State' in sub_group and 'Foreign' in all_data[loc+5]:    #deals with suppressed foreign designation
                        foreign_supp = all_data[loc+5].strip().split()
                        foreign_supp.insert(0,state)
                        foreign_supp.insert(1,county)
                        foreign_supp.insert(2,supp_st)
                        foreign_supp.insert(3,foreign)
                        del foreign_supp[-1]
                        del foreign_supp[-2]
                        foreign_supp.insert(4,state_abbrv)
                        #print(foreign_supp)
                        out_rows.append(foreign_supp)

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
        
        #print(len(out_rows))

        print(f(len(all_data),len(out_rows)))

        for objects in out_rows:
            cur.execute('INSERT INTO %s VALUES(%s)' %(header_out,newmark),objects)
            con.commit()

            
cur.execute('ALTER TABLE %s ADD COLUMN Unique_ID TEXT' %header_out)
cur.execute('UPDATE %s SET Unique_ID = State_FIPS_Origin || County_FIPS_Origin || "_" || State_FIPS_Destination || County_FIPS_Destination' %header_out)
con.commit()

con.close()

print ('done')
