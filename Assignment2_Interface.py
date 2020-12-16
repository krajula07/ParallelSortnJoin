#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    con = openconnection.cursor()

    noThreads = 5
    RANGE_TABLE_PREFIX = "range_partition_"  
    for k in range(noThreads):
        dQuery = "drop table if exists "+(RANGE_TABLE_PREFIX + str(k))+";"
        con.execute(dQuery)
    print("kavya")
    for k in range(noThreads):
        print("kavya")
        con.execute("create table if not exists " +(RANGE_TABLE_PREFIX + str(k))+ " as select * from " + InputTable + " LIMIT 0;" )          
        
     
    
    maxQ="select MAX("+SortingColumnName+") from "+InputTable+";"
    minQ= "select MIN("+SortingColumnName+") from "+InputTable+";"
    con.execute(maxQ)
    maxValue = con.fetchone()[0]
    con.execute(minQ)
    minValue = con.fetchone()[0]
    rInterval = float((maxValue - minValue))/5
    tSort= [0] * 5
    newMin = minValue
    newMax = newMin + rInterval
    for k in range(noThreads):
        if k == 0:
            print(newMin)
            Query = "insert into "+(RANGE_TABLE_PREFIX + str(k))+" (select * from "+InputTable+" where "+SortingColumnName+" >= "+str(newMin)+" and "+SortingColumnName+" <= "+str(newMax)+" order by "+SortingColumnName + " ASC);" 
        else:
            print(newMin)
            Query = "insert into "+(RANGE_TABLE_PREFIX + str(k))+" (select * from "+InputTable+" where "+SortingColumnName+" > "+str(newMin)+" and "+SortingColumnName+" <= "+str(newMax)+" order by "+SortingColumnName + " ASC);"
         
        tSort[k] = threading.Thread(target = con.execute(Query))
        tSort[k].start()
        newMin = newMax
        newMax = newMin + rInterval
    for k in range(noThreads):
        tSort [k].join()
        
    dropTQuery = "drop table if exists "+OutputTable+" ;"
    con.execute(dropTQuery)

    createTQuery = "create table if not exists "+OutputTable+" as select * from "+InputTable+" LIMIT 0 ;"
    con.execute(createTQuery)   

    for i in range(noThreads):
        #insert all sorted tables in this query
        insertTQuery = "insert into "+OutputTable+" select * from "+(RANGE_TABLE_PREFIX+str(i))+";"
        con.execute(insertTQuery)         
    
    
    
    

        
    openconnection.commit()
        
        
        
        
        
        
      

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    con = openconnection.cursor()
    noThreads = 5
    JOIN_INPUTTABLE1_PREFIX = "join_input1_"
    JOIN_INPUTTABLE2_PREFIX ="join_input2_"
    
    oQuery = "create table "+OutputTable+" as select * from "+InputTable1+" INNER JOIN "+InputTable2+" on "+InputTable1+"."+Table1JoinColumn+" = "+InputTable2+"."+Table2JoinColumn+" limit 0;"
    con.execute(oQuery)
    
    minInput1="select MIN("+Table1JoinColumn+") from "+InputTable1+";"
    con.execute(minInput1)
    minValueIT1 = con.fetchone()[0]

    minInput2="select MIN("+Table2JoinColumn+") from "+InputTable2+";"
    con.execute(minInput2)
    minValueIT2 = con.fetchone()[0] 
    
    minValue=min(minValueIT1,minValueIT2)
    
    maxInput1="select MAX("+Table1JoinColumn+") from "+InputTable1+";"
    con.execute(maxInput1)
    maxValueIT1 = con.fetchone()[0]
    
    maxInput2="select MAX("+Table2JoinColumn+") from "+InputTable2+";"
    con.execute(maxInput2)
    maxValueIT2 = con.fetchone()[0]
    
    
    maxValue = max(maxValueIT1,maxValueIT2)
    
    
        
    rInterval = float((maxValue - minValue)/5)
    tSort= [0] * 5
    newMin = minValue
    newMax = newMin + rInterval
    
    for k in range(noThreads):
        
        ''' "insert into "+(JOIN_INPUTTABLE1_PREFIX + str(k))+" (select * from "+InputTable1+" where "+Table1JoinColumn+" >= "+str(newMin)+" and "+Table1JoinColumn+" <= "+str(newMax)+ ");" ''' 
        tSort[k] = threading.Thread(target = RangePartitions,args = (InputTable1,InputTable2,k,newMin , newMax, Table1JoinColumn, Table2JoinColumn, openconnection))
        tSort[k].start()
        newMin = newMax
        newMax = newMin + rInterval
    
     
    jOutput = "join_table_partition_"
    for i in range(noThreads):
        tSort [i].join()
        print("checking...",i)
        finalQuery = "insert into "+OutputTable+" (select * from "+(jOutput+str(i))+") ;"
        con.execute(finalQuery)
        print("lol")
    
    
    
    
    openconnection.commit()
    
    
def RangePartitions(InputTable1,InputTable2 ,k, newMin,newMax,Table1JoinColumn,Table2JoinColumn, openconnection):
    con = openconnection.cursor()
    
    
    
    JOIN_INPUTTABLE1_PREFIX = "join_input1_"
    JOIN_INPUTTABLE2_PREFIX ="join_input2_"
    jOutput = "join_table_partition_"
    con.execute("create table if not exists " +(JOIN_INPUTTABLE1_PREFIX+ str(k))+ " as select * from " + InputTable1 + " LIMIT 0;" ) 
    con.execute("create table if not exists " +(JOIN_INPUTTABLE2_PREFIX+ str(k))+ " as select * from " + InputTable2 + " LIMIT 0;" ) 
    
        
    if k == 0:
        print(newMin)
        con.execute("insert into "+(JOIN_INPUTTABLE1_PREFIX + str(k))+" (select * from "+InputTable1+" where "+Table1JoinColumn+" = "+str(newMin)+ ");" )
        con.execute( "insert into "+(JOIN_INPUTTABLE2_PREFIX + str(k))+" (select * from "+InputTable2+" where "+Table2JoinColumn+" = "+str(newMin)+");" )
    print(newMin)
    con.execute("insert into "+(JOIN_INPUTTABLE1_PREFIX + str(k))+" (select * from "+InputTable1+" where "+Table1JoinColumn+" > "+str(newMin)+" and "+Table1JoinColumn+" <= "+str(newMax)+");")
    con.execute("insert into "+(JOIN_INPUTTABLE2_PREFIX + str(k))+" (select * from "+InputTable2+" where "+Table2JoinColumn+" > "+str(newMin)+" and "+Table2JoinColumn+" <= "+str(newMax)+");")
    
    con.execute("create table if not exists "+(jOutput+str(k))+" as select * from "+(JOIN_INPUTTABLE1_PREFIX+str(k))+" INNER JOIN "+ JOIN_INPUTTABLE2_PREFIX+str(k)+" ON "+ JOIN_INPUTTABLE1_PREFIX+str(k)+"."+Table1JoinColumn+" = "+ JOIN_INPUTTABLE2_PREFIX+str(k)+"."+Table2JoinColumn+" ;")
    
        
    
    openconnection.commit() 
   


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


