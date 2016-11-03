SELECT DISTINCT Store
FROM TemporalData T1
WHERE T1.UnemploymentRate>=10
AND NOT EXISTS (
	SELECT * 
    	FROM TemporalData T2 
    	WHERE T2.FuelPrice>4 AND T2.Store=T1.Store);
