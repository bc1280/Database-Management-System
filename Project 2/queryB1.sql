SELECT AMC.fname, AMC.lname
FROM (
	SELECT A.fname, A.lname, COUNT(DISTINCT C.mid) AS cast_count 
	FROM actor A, (SELECT id FROM movie WHERE year=2010) M, casts C 
	WHERE A.id=C.pid AND M.id=C.mid
	GROUP BY A.id) AMC
WHERE AMC.cast_count>=10;
