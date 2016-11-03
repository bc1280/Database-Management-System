SELECT COUNT(DISTINCT I.pid)
FROM (	SELECT DISTINCT F.mid AS mid
	FROM casts F, (	SELECT DISTINCT D.pid AS pid
			FROM casts D, (	SELECT DISTINCT C.mid AS mid
					FROM (	SELECT id
						FROM actor A 
  						WHERE A.fname="Kevin" AND A.lname="Bacon") K, casts C
  					WHERE K.id=C.pid) E 
  			WHERE D.mid=E.mid) G
  	WHERE F.pid=G.pid
	AND F.mid NOT IN (	SELECT DISTINCT C.mid AS mid
				FROM (	SELECT id
					FROM actor A 
  					WHERE A.fname="Kevin"
					AND A.lname="Bacon") K, casts C
  				WHERE K.id=C.pid)) H, casts I
	WHERE H.mid=I.mid
	AND I.pid NOT IN (	SELECT DISTINCT D.pid AS pid
				FROM casts D, (	SELECT DISTINCT C.mid AS mid
						FROM (	SELECT id
							FROM actor A 
  							WHERE A.fname="Kevin"
							AND A.lname="Bacon") K, casts C
  						WHERE K.id=C.pid) E 
  				WHERE D.mid=E.mid);

