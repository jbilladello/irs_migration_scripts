#Verifies that there are no duplicate records and no footnotes within data

import sqlite3

con = sqlite3.connect('') #Provide path to database
cur = con.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tabnames = cur.fetchall()
problems={}

print('table, unique, distinct, duplicates')

for tab in tabnames:
    cur.execute("SELECT uid FROM %s;" % tab)
    unid=cur.fetchall()
    cur.execute("SELECT DISTINCT uid FROM %s;" % tab)
    dtid=cur.fetchall()
    print(tab[0], len(unid), len(dtid), len(unid) - len(dtid))
    if len(unid) - len(dtid) > 0:
        statement='''SELECT * FROM {0} WHERE uid IN (SELECT uid FROM {0}
        GROUP BY uid HAVING count(uid) > 1);'''.format(tab[0])
        cur.execute(statement)
        dups=cur.fetchall()
        problems[tab[0]]=dups

for key, value in problems.items():
    print ('Duplicates for table ',key)
    for row in value:
        print(row)

print('table, returns -1, exemptions -1, income -1')

for tab in tabnames:
    cur.execute("SELECT uid FROM %s WHERE returns in (-1,'d');" % tab)
    minus_r=cur.fetchall()
    cur.execute("SELECT uid FROM %s WHERE exemptions in (-1,'d');" % tab)
    minus_e=cur.fetchall()
    cur.execute("SELECT uid FROM %s WHERE income in (-1,'d');" % tab)
    minus_i=cur.fetchall()
    print(tab[0], len(minus_r), len(minus_e), len(minus_i))
    
con.close()


