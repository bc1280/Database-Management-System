SELECT DISTINCT D.fname, D.lname
FROM (
	SELECT pid AS actorID, mid AS movieID, did AS directorID
	FROM (casts NATURAL JOIN movie_directors)) CMD, actor A, (SELECT id FROM movie WHERE year=2000) M, directors D
WHERE CMD.actorID = A.id
AND CMD.movieID = M.id
AND CMD.directorID = D.id
AND A.fname = D.fname
AND A.lname = D.lname;
