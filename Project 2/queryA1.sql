SELECT Store, SUM(WeeklySales)
FROM (Holidays NATURAL JOIN Sales)
WHERE IsHoliday='TRUE'
GROUP BY Store
ORDER BY SUM(WeeklySales) DESC
LIMIT 1;

SELECT Store, SUM(WeeklySales)
FROM (Holidays NATURAL JOIN Sales)
WHERE IsHoliday='TRUE'
GROUP BY Store
ORDER BY SUM(WeeklySales) ASC
LIMIT 1;

