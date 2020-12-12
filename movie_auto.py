import sqlite3 as lite
import csv
import re
import pandas as pd
import argparse
import collections
import json
import glob
import math
import os
import requests
import string
import sqlite3
import sys
import time
import xml


class Movie_auto(object):
    def __init__(self, db_name):
        db_name: "cs1656-public.db"
        self.con = lite.connect(db_name)
        self.cur = self.con.cursor()
    
    #q0 is an example 
    def q0(self):
        query = ''' SELECT COUNT(*) FROM Actors'''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q1(self):
        query = '''
        select a.fname, a.lname
        from Cast c
        inner join ACTORS a on c.aid = a.aid
        where c.aid in (
        select c.aid
        from cast c 
        inner join movies m on m.mid = c.mid
        where m.year > 1979 and m.year < 1991
        )
        and c.aid in(
        select c.aid
        from cast c 
        inner join movies m on m.mid = c.mid
        where m.year >= 2000)
        group by a.lname,a.fname
        order by a.lname,a.fname
           
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows
        

    def q2(self):
        query = '''
        select m.title, m.year
        from Movies m    
        where m.year in(
        select m.year
        from movies m 
        where m.title= "Rogue One: A Star Wars Story"
        )
        and m.rank >(
        select m.rank
        from movies m
        where m.title = "Rogue One: A Star Wars Story"
        )
        group by m.title
        order by m.title
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q3(self):
        self.cur.execute('DROP VIEW IF EXISTS movie_count;')
        self.cur.execute('create view movie_count as SELECT c.aid, count(c.role) as num FROM Movies m, Cast c WHERE m.mid = c.mid AND m.title LIKE \"%Star Wars%\" GROUP BY c.aid HAVING num >= 1')

        query = '''
        SELECT a.fname, a.lname, mc.num
	FROM Actors a, movie_count mc
	WHERE a.aid = mc.aid
	group by a.lname, a.fname
	ORDER BY  mc.num DESC, a.lname, a.fname
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows


    def q4(self):
        self.cur.execute('DROP VIEW IF EXISTS PRE1985')
        self.cur.execute(
            'CREATE VIEW PRE1985 AS SELECT a1.fname AS first, a1.lname AS last, a1.aid as aid, m1.year AS year FROM Actors a1, Movies m1, Cast c1 WHERE m1.mid = c1.mid AND a1.aid = c1.aid;')
        query = '''
        SELECT p1.first, p1.last
        FROM PRE1985 p1
        INNER JOIN (SELECT aid, MAX(year) As old FROM PRE1985 GROUP BY aid) p2 ON p1.aid = p2.aid AND p1.year = p2.old
        WHERE p1.year < 1980
        order by p1.last, p1.first
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q5(self):
        query = '''
        select d.fname,d.lname, count(md.mid) as cnt
        from directors d
        join movie_director md
        on d.did = md.did 
        group by d.fname, d.lname
        order by cnt desc, d.lname, d.fname
        limit 10
        
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q6(self):
        query = '''
                select m.title, count(c.aid) as cnt
                from movies m 
                inner join cast c 
                on c.mid = m.mid
                group by m.title
                having cnt >= (
                select min(num_cast2)
                    from (select
                            count(c2.aid) as num_cast2
                            from movies m2
                            inner join cast c2 on c2.mid = m2.mid
                            group by m2.mid
                            order by num_cast2 desc 
                            limit 10
                        )
                    )  
                order by cnt desc 
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q7(self):
        query = '''
         WITH female AS (SELECT mid, COUNT(*) as Fnum 
            FROM Cast C JOIN Actors A on C.aid = A.aid
            WHERE A.gender = 'Female'
            GROUP By mid)
        select K.title as title, D.Fnum as female_num, D.Mnum as male_num From Movies K JOIN (
            select F.mid,
            CASE WHEN Mnum is NULL THEN 0 ELSE Mnum end AS Mnum, 
            CASE WHEN Fnum is NULL THEN 0 ELSE Fnum end AS Fnum 
            From female F LEFT OUTER JOIN (SELECT mid, COUNT(*) as Mnum
                FROM Cast C JOIN Actors A on C.aid = A.aid
                WHERE A.gender = 'Male'
                GROUP By mid) M on M.mid = F.mid) D on D.mid = K.mid
            WHERE D.Fnum > D.Mnum
        order by K.title
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q8(self):
        query = '''
        SELECT fname, lname, c_d FROM (
            SELECT aid, fname, lname, count(distinct did) as c_d FROM (SELECT * FROM (SELECT * 
            FROM (SELECT * FROM Actors A JOIN Cast C on A.aid = C.aid) AC 
            JOIN Movies M on M.mid = AC.mid) ACM JOIN movie_director MD on MD.mid = ACM.mid)
            GROUP BY aid, fname, lname	
            HAVING c_d >= 7)
            
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows


    def q9(self):
        self.cur.execute('DROP VIEW IF EXISTS debut_count')
        self.cur.execute('create view debut_count as SELECT a.aid, min(m.year) as yc from Movies m, Actors a, Cast c WHERE m.mid = c.mid AND a.aid = c.aid AND c.aid IN(SELECT a1.aid from Actors a1 WHERE lower(a1.fname) LIKE \'D%\' )group by a.aid')
        query = '''
        SELECT a.fname, a.lname, count(q.aid)
			FROM debut_count q, Actors a, Cast c, Movies m
			WHERE q.aid = a.aid AND q.yc = m.year AND c.mid = m.mid AND c.aid = q.aid
			GROUP BY q.aid
			ORDER BY count(q.aid) desc  
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q10(self):
        query = '''
        SELECT a.lname, m.title
	FROM Actors AS a
	INNER JOIN Cast AS c ON a.aid = c.aid
	INNER JOIN Movies AS m ON c.mid = m.mid
	INNER JOIN Movie_Director AS md ON c.mid = md.mid
	INNER JOIN Directors AS d ON d.did = md.did
	WHERE a.lname = d.lname
	ORDER BY a.lname    
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q11(self):
        self.cur.execute('DROP VIEW IF EXISTS bacon1')
        self.cur.execute('create view bacon1 as SELECT DISTINCT c1.aid from Cast c1 where c1.mid in (select c.mid from cast c where c.aid = (select a.aid from Actors A where a.lname="Bacon"))')

        self.cur.execute('DROP VIEW IF EXISTS bacon2')
        self.cur.execute('create view bacon2 as SELECT DISTINCT c1.aid from Cast c1 where c1.mid in (select c.mid from Cast c where c.aid in (Select * from bacon1))')

        query = '''
        select a1.fname, a1.lname
        from bacon2 b, Actors a1
        where b.aid = a1.aid and b.aid not in (Select a.aid from Actors a where a.lname= 'Bacon')
        group by a1.fname, a1.lname
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q12(self):
        query = '''
         SELECT a.fname, a.lname, COUNT(m.mid), AVG(m.rank) AS popularity
	FROM Actors AS a
	INNER JOIN Cast AS c ON a.aid = c.aid
	INNER JOIN Movies AS m ON c.mid = m.mid
	GROUP BY a.aid
	ORDER BY popularity DESC
	LIMIT 20  
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

if __name__ == "__main__":
    task = Movie_auto("cs1656-public.db")
    rows = task.q0()
    print(rows)
    print()
    rows = task.q1()
    print(rows)
    print()
    rows = task.q2()
    print(rows)
    print()
    rows = task.q3()
    print(rows)
    print()
    rows = task.q4()
    print(rows)
    print()
    rows = task.q5()
    print(rows)
    print()
    rows = task.q6()
    print(rows)
    print()
    rows = task.q7()
    print(rows)
    print()
    rows = task.q8()
    print(rows)
    print()
    rows = task.q9()
    print(rows)
    print()
    rows = task.q10()
    print(rows)
    print()
    rows = task.q11()
    print(rows)
    print()
    rows = task.q12()
    print(rows)
    print()
