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
,month(fecha_inicio) asc; 

/* Consulta para verificar la razón entre datos con hora de actualización y sin ella */

with con_hora AS (
	SELECT 
	fecha_inicio
	,count(*) [cantidad_con]
	FROM Estudios.dbo.Bencina93Hist
	WHERE hora_actualizacion IS NOT NULL
	group by fecha_inicio
	),
	sin_hora AS (
	SELECT 
	fecha_inicio
	,count(*) [cantidad_sin]
	FROM Estudios.dbo.Bencina93Hist
	WHERE hora_actualizacion IS NULL
	group by fecha_inicio
	)
SELECT 
c.fecha_inicio
,s.cantidad_sin
,c.cantidad_con
,(CAST(c.cantidad_con as FLOAT) /(CAST(s.cantidad_sin as FLOAT)+CAST(c.cantidad_con as FLOAT)))*100 [cobertura_hora]
from con_hora as c
inner join sin_hora as s on c.fecha_inicio=s.fecha_inicio
order by fecha_inicio asc; 

select *from Estudios.dbo.Bencina93Hist where fecha_inicio='2024-07-17';
select *from Estudios.dbo.Bencina95Hist where fecha_inicio='2024-07-17';
select *from Estudios.dbo.Bencina97Hist where fecha_inicio='2024-07-17';
select *from Estudios.dbo.DieselHist where fecha_inicio='2024-07-17';

select *from Estudios.dbo.Bencina93Hist where fecha_fin='2024-07-17';
select *from Estudios.dbo.Bencina95Hist where fecha_fin='2024-07-17';
select *from Estudios.dbo.Bencina97Hist where fecha_fin='2024-07-17';
select *from Estudios.dbo.DieselHist where fecha_fin='2024-07-17';



