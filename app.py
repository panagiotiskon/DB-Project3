#PANAGIOTIS KONTOEIDIS 1900266
#GIORGOS PANTELAKIS 1900140


# ----- CONFIGURE YOUR EDITOR TO USE 4 SPACES PER TAB ----- #
from typing import final
from sqlalchemy import null
from random import seed
from random import randint
import settings
import sys,os
sys.path.append(os.path.join(os.path.split(os.path.abspath(__file__))[0], 'lib'))
import pymysql as db



def connection():
    ''' User this function to create your connections '''
    con = db.connect(
        settings.mysql_host, 
        settings.mysql_user, 
        settings.mysql_passwd, 
        settings.mysql_schema)
    
    return con

def findAirlinebyAge(x,y):
    
    # Create a new connection
    con=connection()
    # Create a cursor on the connection
    cur=con.cursor()

    if(x>y):
        return [("X must be bigger than Y",),]

    sql = """select air.name, count(*)
            from flights as f, passengers as p , flights_has_passengers as fp, routes as r, airlines as air
            where f.routes_id = r.id and r.airlines_id = air.id and p.id = fp.passengers_id and f.id = fp.flights_id and 2022-p.year_of_birth>%s and 2022-p.year_of_birth<%s
            group by air.id;
        """
    vars = (x,y)

    cur.execute(sql, vars)

    airnames = cur.fetchall()

    if(airnames==()):
        return[("Passengers not found for the ages given",)]

    temp = None

    #save the airline with the most passengers in res var

    for name in airnames:
        if(temp is None or name[1]> temp):
            temp = name[1]
            res = [name[0], name[1]]


    sql2 = """select count(*)
              from airlines as air, airlines_has_airplanes airpl, airplanes as pl
              where air.id = airpl.airlines_id and airpl.airplanes_id = pl.id and air.name = %s
              group by air.name"""

    cur.execute(sql2, res[0])

    count = cur.fetchall()
    
    result = res[0],res[1],count[0][0]

    return [("airline_name","num_of_passengers", "num_of_aircrafts"),result]


def findAirportVisitors(x, a, b):

   # Create a new connection
    con = connection()

    # Create a cursor on the connection
    cur = con.cursor()

    # sql query
    sql = """select airp.name ,count(fp.passengers_id)
             from flights f,flights_has_passengers fp,airlines air,routes r,airports airp
             where air.name=%s and f.date>=%s and f.date<=%s and f.routes_id=r.id and r.airlines_id=air.id and airp.id=r.destination_id  and fp.flights_id=f.id
             group by r.destination_id
             order by count(fp.passengers_id) desc ;"""
    vars = (x, a, b)
    cur.execute(sql, vars)

    results = cur.fetchall()

    # If there aren't results will return "Empty"
    if (results == ()):
        return[("Empty",)]

    return [("aiport_name", "number_of_visitors"), ] + list(results)


def findFlights(x,a,b):

    # Create a new connection
    con=connection()
    # Create a cursor on the connection
    cur=con.cursor()

    sql = """select f.id, airlines.alias, air2.name, airplanes.model
            from flights as f, routes as r, airports as air, airports as air2, airlines, airplanes
            where airlines.active='Y' and f.routes_id = r.id and r.source_id = air.id and f.date = %s and air.city = %s and air2.city = %s and air2.id = r.destination_id and airlines.id = r.airlines_id and airplanes.id = f.airplanes_id;"""
    
    vars = (x,a,b)
    cur.execute(sql, vars)
    res =  cur.fetchall()
    return [("flight_id", "alt_name", "dest_name", "aircraft_model"),res[0]]
    

def findLargestAirlines(N):
    # Create a new connection
    con = connection()

    # Create a cursor on the connection
    cur = con.cursor()

    # Sort all the airlines according the flights the have
    sql = """select air.name,air.id,count(airp.id),count(distinct f.id)
             from airlines air,routes r,airlines_has_airplanes aha,airplanes airp,flights f
             where air.id=r.airlines_id and aha.airlines_id=air.id and aha.airplanes_id=airp.id and f.routes_id=r.id
             group by air.id
             order by count( distinct f.id) desc"""

    cur.execute(sql)
    result = cur.fetchall()
    print(result)

    i = int(N)
    # If N=0 will return "Empty"
    if (i == 0):
        return[("Empty",)]

    # if the Nth airport has the same number of fligths with the (N+1)th airport print and print an the (N+1)th airpor

    for count, row in enumerate(result):
        if count == i-1:
            old_row = row
        if count == i:
            if old_row[3] == row[3]:
                i += 1
    new_result = (result[0:i])

    return [("name", "id", "num_of_aircrafts", "num_of_flights"), ] + list(new_result)

    
def insertNewRoute(x,y):
    # Create a new connection
    con=connection()
    seed(1)
    # Create a cursor on the connection
    cur=con.cursor()

    #check if X airline exists

    sql = """select distinct air.name
            from airports as air, routes as r, airlines
            where r.source_id = air.id and airlines.id = r.airlines_id and airlines.alias = %s"""
    
    vars  = (x,y)

    cur.execute(sql, x)
    res =  cur.fetchall()
    if(res==()):
        return[("Alias not found, try again",)]

    #check if Y airport is an airport operated from the airline as a source

    sql2 = """select distinct air.name
            from airports as air, routes as r, airlines
            where r.source_id = air.id and airlines.id = r.airlines_id and airlines.alias = %s and air.name = %s"""

    cur.execute(sql2, vars)
    res2 =  cur.fetchall()
    
    if(res2==()):
        return[("Departure Airport not found, try again",)]
    
    #from the airport Y take all the possible destinations(cities) that airline X does not have a route to

    sql3 = """select distinct air.city
                from airports as air 
                where air.city not in (select  distinct air2.city
					                    from airports as air, routes as r, airlines, airports as air2
					                    where r.source_id = air.id and airlines.id = r.airlines_id and airlines.alias = %s and air2.id =  r.destination_id and air.name = %s);"""
    

    cur.execute(sql3, vars)
    res3 =  cur.fetchall()

    if(res3==()):
        return[("Airline capacity full",)]


    #take the maximum route.id so that when new route is inserted new route.id = route.id+1

    sql5 = """select max(routes.id)
                        from routes;"""

    cur.execute(sql5)
    res5 = cur.fetchone()

    route_counter=int(res5[0])

    route_counter+=1
           
    #choose a random city destination 

    city = res3 [randint(0, len(res3)-1)]

    #take the airport id of the destination

    sql4 ="""select  airports.id
            from airports
            where airports.city = %s"""

    cur.execute(sql4, city[0])
    destination_ids = cur.fetchall()

    #if a destination has more that one airport then choose one random airport from that destination

    if(len(destination_ids)>1):

        destination_id = destination_ids[randint(0, len(destination_ids)-1)]

        #insert the new route

        final_sql = """insert into routes 
                                values(%s,(select air.id from airlines as air where air.alias = %s) , (select airports.id from airports where airports.name = %s), %s); """
                
        var = (route_counter,x,y,destination_id)
        cur.execute(final_sql, var)
        return[("ΟΚ",)]

    else:
        final_sql = """insert into routes 
                                values(%s,(select air.id from airlines as air where air.alias = %s) , (select airports.id from airports where airports.name = %s), %s); """
        var = (route_counter,x,y,destination_ids)
        cur.execute(final_sql, var)
        return[("ΟΚ",)]