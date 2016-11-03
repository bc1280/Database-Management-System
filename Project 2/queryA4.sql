SELECT COUNT(A.WeekDate)
FROM (	SELECT WeekDate, SUM(WeeklySales) AS OverallWeeklySales 
	FROM (Holidays NATURAL JOIN Sales) C 
	WHERE C.IsHoliday='FALSE' 
	GROUP BY WeekDate) A, (	SELECT SUM(WeeklySales)/COUNT(DISTINCT(WeekDate)) AS AvgHolidaySales
				FROM (Holidays NATURAL JOIN Sales) D 
				WHERE D.IsHoliday='TRUE') B 
WHERE A.OverallWeeklySales > B.AvgHolidaySales;
