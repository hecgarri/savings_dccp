SELECT *
FROM estudios.dbo.Bencina93Hist
WHERE YEAR(fecha_inicio) = 2024
  AND fecha_inicio = (SELECT MAX(fecha_inicio) 
                      FROM estudios.dbo.Bencina93Hist 
                      WHERE YEAR(fecha_inicio) = 2024);

SELECT *
FROM estudios.dbo.Bencina95Hist
WHERE YEAR(fecha_inicio) = 2024
  AND fecha_inicio = (SELECT MAX(fecha_inicio) 
                      FROM estudios.dbo.Bencina95Hist 
                      WHERE YEAR(fecha_inicio) = 2024);

SELECT *
FROM estudios.dbo.Bencina97Hist
WHERE YEAR(fecha_inicio) = 2024
  AND fecha_inicio = (SELECT MAX(fecha_inicio) 
                      FROM estudios.dbo.Bencina97Hist 
                      WHERE YEAR(fecha_inicio) = 2024);

SELECT *
FROM estudios.dbo.DieselHist
WHERE YEAR(fecha_inicio) = 2024
  AND fecha_inicio = (SELECT MAX(fecha_inicio) 
                      FROM estudios.dbo.DieselHist 
                      WHERE YEAR(fecha_inicio) = 2024);

SELECT 
	id
	,fecha_inicio
	,precio
	FROM Estudios.dbo.Bencina93Hist 
	where fecha_inicio >= '2024-05-23' 
	--,year(fecha_inicio) 
	order by 
	fecha_inicio asc;


SELECT 
year(fecha_inicio) 
,month(fecha_inicio)
,count(*)
from estudios.dbo.Bencina93Hist
group by 
year(fecha_inicio) 
,month(fecha_inicio)
order by 
year(fecha_inicio) asc
,month(fecha_inicio) asc