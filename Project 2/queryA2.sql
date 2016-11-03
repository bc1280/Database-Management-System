SELECT Dept,  SUM(WeeklySales/Size) AS NormalizedTotalSales
FROM (Stores NATURAL JOIN Sales)
GROUP BY Dept
ORDER BY NormalizedTotalSales DESC
LIMIT 20;
