
import sys
import psycopg2
import timeit
import numbers
import time
DATABASE_NAME = 'dds_assgn1'

lastroundrobintable = 0;
roundrobinpartitions = 0;
rangepartitions = 0;

def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


"""This Method reads the data from the input file and creates a table with the given params"""
def loadratings(ratingstablename, ratingsfilepath, openconnection):
    print "started loading data into table"
    cur = openconnection.cursor()
    #Creating a table in the name given as parameter to the function
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)
    cur.execute("CREATE TABLE " + ratingstablename + " (UserID INT, temp1 VARCHAR(10),  MovieID INT , temp3 VARCHAR(10),  Rating REAL, temp5 VARCHAR(10), Timestamp INT)")
    #opening the input file in  readonly mode
    loadout = open(ratingsfilepath, 'r')
    #Loading the data into respective column
    cur.copy_from(loadout, ratingstablename, sep=':', columns=('UserID', 'temp1', 'MovieID', 'temp3', 'Rating', 'temp5', 'Timestamp'))
    cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN temp1, DROP COLUMN temp3,DROP COLUMN temp5, DROP COLUMN Timestamp")
    print "done loading!"
    cur.close()
    openconnection.commit()


"""This method partitions the table based on the range column """
def rangepartition(ratingstablename, numberofpartitions, openconnection):
    print "range partition method"
    name = "range_part"
    global rangepartitions;
    rangepartitions = numberofpartitions
    if (numberofpartitions < 0 or not isinstance(numberofpartitions, numbers.Integral)):
        return;

    try:
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" % ratingstablename)

        if not bool(cursor.rowcount):
            print "The Table is not loaded. please load it"
            return

        cursor.execute( "SELECT * FROM %s" % (ratingstablename))
        tablevalues = cursor.fetchall();

        insert = []

        minR = 0.0
        maxR = 5.0
        step = (maxR - minR) / (float)(numberofpartitions)
        tableNum = 0;
        while tableNum < numberofpartitions:
            newname = name + `tableNum`
            cursor.execute("CREATE TABLE IF NOT EXISTS %s(UserID INT, MovieID INT, Rating REAL)" % (newname))
            tableNum += 1;

        tableNum = 0;
        while minR < maxR:
            insert = []
            newname = name + `tableNum`
            if minR == 0.0:
                for value in tablevalues:
                    if value[2]>=minR and value[2]<=(minR + step):
                        insert.append(value)

            elif minR != 0.0:
                for value in tablevalues:
                    if value[2]>minR and value[2]<=(minR + step):
                        insert.append(value)
            query = "INSERT INTO " + newname + "(UserID, MovieID, Rating) VALUES(%s, %s, %s)"
            cursor.executemany(query, insert)
            minR = (minR + step)
            tableNum = tableNum + 1

        print "range partition done"
        #cursor.copy_to(sys.stdout, "range_part0", sep='\t')
        openconnection.commit()

    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)

    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)

    finally:
        if cursor:
            cursor.close()

"""This method partittions the table round robin fashion"""
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    print "round robin partition method"
    name = "rrobin_part"
    try:
        global roundrobinpartitions
        roundrobinpartitions = numberofpartitions
        if (numberofpartitions < 0 or not isinstance(numberofpartitions, numbers.Integral)):
            return;

        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" % (ratingstablename))
        if not bool(cursor.rowcount):
            print "Please Load Ratings Table first!!!"
            return

        cursor.execute("SELECT * FROM %s" % ratingstablename)
        rows = cursor.fetchall()

        tableNum = 0
        while tableNum < numberofpartitions:
            newTableName = name + `tableNum`
            cursor.execute("CREATE TABLE IF NOT EXISTS %s(UserID INT, MovieID INT, Rating REAL)" % (newTableName))
            tableNum += 1;

        lastInserted = 0
        for row in rows:
            newTableName = name + `lastInserted`
            cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (newTableName, row[0], row[1], row[2]))
            lastInserted = (lastInserted + 1) % numberofpartitions

        global lastroundrobintable
        lastroundrobintable = lastInserted
        print "round robin partition done"
        openconnection.commit()

    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    print "round robin insert"
    try:
        global  roundrobinpartitions
        if (roundrobinpartitions < 0 or not isinstance(roundrobinpartitions, numbers.Integral) or rating < 0):
            return;
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" % (ratingstablename))
        if not bool(cursor.rowcount):
            print "Please Load Ratings Table first!!!"
            return

        global lastroundrobintable
        name = "rrobin_part" + `lastroundrobintable`
        cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (name, userid, itemid, rating))
        lastroundrobintable  = (lastroundrobintable + 1) % roundrobinpartitions
        #cursor.copy_to(sys.stdout, name, sep="\t")
        openconnection.commit()

    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    print "range insert"
    try:
        global rangepartitions
        if (rangepartitions < 0 or not isinstance(rangepartitions, numbers.Integral) or rating < 0):
            return;
        cursor = openconnection.cursor()
        cursor.execute("select * from information_schema.tables where table_name='%s'" % ratingstablename)
        if not bool(cursor.rowcount):
            print "Please Load Ratings Table first!!!"
            return
        minR = 0.0
        maxR = 5.0
        step = (maxR - minR) / (float)(rangepartitions)
        tableNumber=0
        temp =  step
        while(tableNumber <= rangepartitions - 1):
            if(rating > temp):
                temp = temp + step
                tableNumber = tableNumber + 1
            else:
                break
        name = "range_part" + `tableNumber`
        cursor.execute("INSERT INTO %s(UserID, MovieID, Rating) VALUES(%d, %d, %f)" % (name, userid, itemid, rating))
        #cursor.copy_to(sys.stdout,name,sep="\t")
        openconnection.commit()

    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)
    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = cursor.fetchall()
        for table_name in tables:
            cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        openconnection.commit()

    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databa787usename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:
        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()
        create_db(DATABASE_NAME)
        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)
        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)
            # Here is where I will start calling your functions to test them. For example,
            loadratings('ratings', 'test_data.dat', con)
            rangepartition('ratings', 10 , con)
            roundrobinpartition('ratings', 10, con)
            roundrobininsert('ratings', 1, 199, 3.75, con)
            rangeinsert('ratings', 1, 188, 0.5, con)
            deleteTables(con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################
            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
