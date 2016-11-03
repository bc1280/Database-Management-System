SELECT M.year, SUM(C.ycount) AS dcount
FROM (	SELECT year, COUNT(id) AS ycount
	FROM movie 
	GROUP BY year) C, (
	SELECT DISTINCT year
	FROM movie) M
WHERE C.year>=M.year AND C.year<M.year+10
GROUP BY M.year
ORDER BY dcount DESC
LIMIT 1;
