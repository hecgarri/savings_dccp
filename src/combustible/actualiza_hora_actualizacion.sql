UPDATE Estudios.dbo.Bencina93Hist
SET hora_actualizacion = aux.hora_actualizacion
FROM Estudios.dbo.Bencina93Aux AS aux
WHERE Estudios.dbo.Bencina93Hist.id = aux.id
AND Estudios.dbo.Bencina93Hist.fecha_inicio = aux.fecha_inicio
AND Estudios.dbo.Bencina93Hist.precio = aux.precio
AND Estudios.dbo.Bencina93Hist.tipo = aux.tipo
AND Estudios.dbo.Bencina93Hist.hora_actualizacion IS NULL
AND Estudios.dbo.Bencina93Hist.fecha_inicio >= '2024-06-18';

UPDATE Estudios.dbo.Bencina95Hist 
SET hora_actualizacion = aux.hora_actualizacion
FROM Estudios.dbo.Bencina95Aux AS aux
WHERE Estudios.dbo.Bencina95Hist.id = aux.id
AND Estudios.dbo.Bencina95Hist.fecha_inicio = aux.fecha_inicio
AND Estudios.dbo.Bencina95Hist.precio = aux.precio
AND Estudios.dbo.Bencina95Hist.tipo = aux.tipo
AND Estudios.dbo.Bencina95Hist.hora_actualizacion IS NULL
AND Estudios.dbo.Bencina95Hist.fecha_inicio >= '2024-06-18';

UPDATE Estudios.dbo.Bencina97Hist
SET hora_actualizacion = aux.hora_actualizacion
FROM Estudios.dbo.Bencina97Aux AS aux
WHERE Estudios.dbo.Bencina97Hist.id = aux.id
AND Estudios.dbo.Bencina97Hist.fecha_inicio = aux.fecha_inicio
AND Estudios.dbo.Bencina97Hist.precio = aux.precio
AND Estudios.dbo.Bencina97Hist.tipo = aux.tipo
AND Estudios.dbo.Bencina97Hist.hora_actualizacion IS NULL
AND Estudios.dbo.Bencina97Hist.fecha_inicio >= '2024-06-18';

UPDATE Estudios.dbo.DieselHist
SET hora_actualizacion = aux.hora_actualizacion
FROM Estudios.dbo.DieselAux AS aux
WHERE Estudios.dbo.DieselHist.id = aux.id
AND Estudios.dbo.DieselHist.fecha_inicio = aux.fecha_inicio
AND Estudios.dbo.DieselHist.precio = aux.precio
AND Estudios.dbo.DieselHist.tipo = aux.tipo
AND Estudios.dbo.DieselHist.hora_actualizacion IS NULL
AND Estudios.dbo.DieselHist.fecha_inicio >= '2024-06-18';


select * from estudios.dbo.Bencina93Hist where fecha_inicio>='2024-06-18'





