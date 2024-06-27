select *
from estudios.dbo.Bencina93Anterior
order by fecha_inicio desc

/*

*/
select 
b93.fecha_inicio
,count(*)
from estudios.dbo.Bencina93Hist as b93
where YEAR(b93.fecha_inicio)='2023'
group by b93.fecha_inicio
order by b93.fecha_inicio desc 


select 
YEAR(b93.fecha_inicio)
,MONTH(b93.fecha_inicio)
,COUNT(*)
from estudios.dbo.Bencina93Hist as b93
where YEAR(b93.fecha_inicio)>='2020'
group by YEAR(b93.fecha_inicio)
,MONTH(b93.fecha_inicio)
order by 
YEAR(b93.fecha_inicio) asc
,MONTH(b93.fecha_inicio) asc

select 
MONTH(b95.fecha_inicio)
,COUNT(*)
from estudios.dbo.Bencina95Hist as b95
where YEAR(b95.fecha_inicio)=2023
group by MONTH(b95.fecha_inicio)
order by MONTH(b95.fecha_inicio) asc


select 
MONTH(b97.fecha_inicio)
,COUNT(*)
from estudios.dbo.Bencina97Hist as b97
where YEAR(b97.fecha_inicio)=2023
group by MONTH(b97.fecha_inicio)
order by MONTH(b97.fecha_inicio) asc



select 
MONTH(di.fecha_inicio)
,COUNT(*)
from estudios.dbo.DieselHist as di
where YEAR(di.fecha_inicio)=2023
group by MONTH(di.fecha_inicio)
order by MONTH(di.fecha_inicio) asc

select * from estudios.dbo.DieselHist
where month(fecha_inicio)='1' and year(fecha_inicio)=2023
order by id asc

select * from estudios.dbo.Bencina93Hist
order by fecha_inicio desc


/*
SELECT * FROM sys.database_permissions
WHERE grantee_principal_id = USER_ID();


SELECT name AS ColumnName, 
       system_type_id AS SystemTypeID, 
       system_type_name AS SystemTypeName
FROM sys.columns
WHERE object_id = OBJECT_ID('Estudios.dbo.Bencina93Hist');
*/

select * 
from  Estudios.dbo.Bencina93Aux