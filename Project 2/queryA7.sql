DROP TABLE IF EXISTS Output;

CREATE TABLE Output(
	AttributeName VARCHAR(20), 
	CorrelationSign Integer
);

INSERT INTO Output VALUES ( "Temperature", (SELECT CASE WHEN SUM((B.temperature-temperature_avg)*(B.weeklysales-weeklysales_avg)) > 0 THEN 1 WHEN SUM((B.temperature-temperature_avg)*(B.weeklysales-weeklysales_avg)) < 0 THEN -1 ELSE 0 END FROM (SELECT AVG(weeklysales) AS weeklysales_avg, AVG(temperature) AS temperature_avg FROM (sales NATURAL JOIN temporaldata)) A, (sales NATURAL JOIN temporaldata) B ) );
INSERT INTO Output VALUES ( "FuelPrice", (SELECT CASE WHEN SUM((B.fuelprice-fuelprice_avg)*(B.weeklysales-weeklysales_avg)) > 0 THEN 1 WHEN SUM((B.fuelprice-fuelprice_avg)*(B.weeklysales-weeklysales_avg)) < 0 THEN -1 ELSE 0 END FROM (SELECT AVG(weeklysales) AS weeklysales_avg, AVG(fuelprice) AS fuelprice_avg FROM (sales NATURAL JOIN temporaldata)) A, (sales NATURAL JOIN temporaldata) B ) );
INSERT INTO Output VALUES ( "CPI", (SELECT CASE WHEN SUM((B.cpi-cpi_avg)*(B.weeklysales-weeklysales_avg)) > 0 THEN 1 WHEN SUM((B.cpi-cpi_avg)*(B.weeklysales-weeklysales_avg)) < 0 THEN -1 ELSE 0 END FROM (SELECT AVG(weeklysales) AS weeklysales_avg, AVG(cpi) AS cpi_avg FROM (sales NATURAL JOIN temporaldata)) A, (sales NATURAL JOIN temporaldata) B ) );
INSERT INTO Output VALUES ( "UnemploymentRate", (SELECT CASE WHEN SUM((B.unemploymentrate-unemploymentrate_avg)*(B.weeklysales-weeklysales_avg)) > 0 THEN 1 WHEN SUM((B.unemploymentrate-unemploymentrate_avg)*(B.weeklysales-weeklysales_avg)) < 0 THEN -1 ELSE 0 END FROM (SELECT AVG(weeklysales) AS weeklysales_avg, AVG(unemploymentrate) AS unemploymentrate_avg FROM (sales NATURAL JOIN temporaldata)) A, (sales NATURAL JOIN temporaldata) B ) );

SELECT * FROM Output;
