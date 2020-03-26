#Copy new tables from temporary SQLite database to permanent one 
#Recreates tables and inserts records to preserve keys and constraints

import sqlite3, os, sys

#MODIFY these values to pull from the right sources

#db1 should be the good, final database
db1=''

#db2 should be the test database
db2=os.path.join('irs1718','test_st1718.sqlite')

#Used for pattern searching to select appropriate tables
tabyears='2017_18'

#Script begins
con = sqlite3.connect(db1)
cur = con.cursor()
cur.execute("ATTACH '{}' AS db2;".format(db2))

tabnames=[]

cur.execute("SELECT name FROM db2.sqlite_master WHERE type='table' AND name LIKE '%{}%';".format(tabyears))
tabgrab = cur.fetchall()
for item in tabgrab:
    tabnames.append(item[0])

print('\nYou are about to add these tables from',db2)
for t in tabgrab:
    print(t) 
answer = input('Are you sure you want to do this? (y/n): ')

if answer=='y':
    for t in tabnames:
        cur.execute("SELECT sql FROM db2.sqlite_master WHERE type='table' AND name = '{}';".format(t))
        create = cur.fetchone()[0]
        cur.execute("SELECT * FROM db2.{};".format(t))
        rows=cur.fetchall()
        rowcount=len(rows)
        colcount=len(rows[0])
        pholder='?,'*colcount
        newholder=pholder[:-1]
        cur.execute(create)
        cur.executemany("INSERT INTO {} VALUES ({});".format(t, newholder),rows)
        con.commit() 
        print('Added table', t, 'with', rowcount,'records and',colcount,'fields')
else:
    print('\nEXITING PROGRAM, no changes made \n')
    con.close()
    sys.exit(0)
    
con.commit()
con.close()
