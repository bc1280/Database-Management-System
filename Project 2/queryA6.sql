DROP TABLE IF EXISTS T1;
DROP TABLE IF EXISTS T2;
DROP TABLE IF EXISTS T3;
DROP TABLE IF EXISTS T4;
DROP TABLE IF EXISTS T5;
DROP TABLE IF EXISTS T6;
DROP TABLE IF EXISTS T7;
DROP TABLE IF EXISTS T8;
DROP TABLE IF EXISTS T9;

CREATE TABLE T1 AS SELECT Store, Dept, Month
			FROM (	SELECT Store, Dept, Month, SUM(WeeklySales) AS MonthlySales
				FROM (	SELECT Store, Dept, SUBSTR(WeekDate,1,7) AS Month, WeeklySales
					FROM Sales)
				GROUP BY Store, Dept, Month)
			WHERE Month Like "%2010%";
CREATE TABLE T2 AS SELECT Store, Dept, COUNT(Month) AS COUNT
			FROM T1
			GROUP BY Store, Dept;
CREATE TABLE T3 AS SELECT DISTINCT Store
			FROM T2
			WHERE COUNT<11;
SELECT *
FROM T1
WHERE NOT EXISTS (	SELECT *
			FROM T3
			WHERE T1.Store=T3.Store);

CREATE TABLE T4 AS SELECT Store, Dept, Month
			FROM (	SELECT Store, Dept, Month, SUM(WeeklySales) AS MonthlySales
				FROM (	SELECT Store, Dept, SUBSTR(WeekDate,1,7) AS Month, WeeklySales
					FROM Sales)
				GROUP BY Store, Dept, Month)
			WHERE Month Like "%2011%";
CREATE TABLE T5 AS SELECT Store, Dept, COUNT(Month) AS COUNT
			FROM T4
			GROUP BY Store,Dept;
CREATE TABLE T6 AS SELECT DISTINCT Store
			FROM T5
			WHERE COUNT<12;
SELECT *
FROM T4
WHERE NOT EXISTS (	SELECT *
			FROM T6
			WHERE T4.Store=T6.Store);

CREATE TABLE T7 AS SELECT Store, Dept, Month
			FROM (	SELECT Store, Dept, Month, SUM(WeeklySales) AS MonthlySales
				FROM (	SELECT Store, Dept, SUBSTR(WeekDate,1,7) AS Month, WeeklySales
					FROM Sales)
				GROUP BY Store, Dept, Month)
			WHERE Month Like "%2012%";
CREATE TABLE T8 AS SELECT Store, Dept, COUNT(Month) AS COUNT
			FROM T7
			GROUP BY Store, Dept;
CREATE TABLE T9 AS SELECT DISTINCT Store
			FROM T8
			WHERE COUNT<10;
SELECT *
FROM T7
WHERE NOT EXISTS (	SELECT *
			FROM T9
			WHERE T7.Store=T9.Store);
