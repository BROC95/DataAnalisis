SELECT * FROM "Ubi" ;SELECT  count(id_provincia), categoria, provincia FROM "Ubi" GROUP BY(provincia), (categoria);SELECT * FROM "Cine";SELECT count(espacio_incaa), sum(pantallas), sum(butacas), provincia  FROM "Cine" GROUP by provincia;