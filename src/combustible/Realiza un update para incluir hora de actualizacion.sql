/*
ALTER TABLE Estudios.dbo.Bencina93Hist
ADD hora_actualizacion TIME;


ALTER TABLE Estudios.dbo.Bencina95Hist
ADD hora_actualizacion TIME;

ALTER TABLE Estudios.dbo.Bencina97Hist
ADD hora_actualizacion TIME;

ALTER TABLE Estudios.dbo.DieselHist
ADD hora_actualizacion TIME;

CREATE TABLE Estudios.dbo.Bencina93Paso(
                    id VARCHAR(50),
                    fecha_inicio DATE,
                    fecha_fin DATE,
                    precio DECIMAL(10,2),
                    tipo VARCHAR(50),
					hora_actualizacion TIME
					)

CREATE TABLE Estudios.dbo.Bencina95Paso(
                    id VARCHAR(50),
                    fecha_inicio DATE,
                    fecha_fin DATE,
                    precio DECIMAL(10,2),
                    tipo VARCHAR(50),
					hora_actualizacion TIME
					)

CREATE TABLE Estudios.dbo.Bencina97Paso(
                    id VARCHAR(50),
                    fecha_inicio DATE,
                    fecha_fin DATE,
                    precio DECIMAL(10,2),
                    tipo VARCHAR(50),
					hora_actualizacion TIME
					)

CREATE TABLE Estudios.dbo.DieselPaso(
                    id VARCHAR(50),
                    fecha_inicio DATE,
                    fecha_fin DATE,
                    precio DECIMAL(10,2),
                    tipo VARCHAR(50),
					hora_actualizacion TIME
					)



UPDATE h
SET h.hora_actualizacion = n.hora_actualizacion
FROM Estudios.dbo.Bencina93Hist as h
JOIN Estudios.dbo.Bencina93Paso as n ON h.fecha_inicio=n.fecha_inicio and h.id=n.id and h.precio=n.precio  
WHERE h.hora_actualizacion IS NULL;


UPDATE h
SET h.hora_actualizacion = n.hora_actualizacion
FROM Estudios.dbo.Bencina95Hist as h
JOIN Estudios.dbo.Bencina95Paso as n ON h.fecha_inicio=n.fecha_inicio and h.id=n.id and h.precio=n.precio  
WHERE h.hora_actualizacion IS NULL;

UPDATE h
SET h.hora_actualizacion = n.hora_actualizacion
FROM Estudios.dbo.Bencina97Hist as h
JOIN Estudios.dbo.Bencina97Paso as n ON h.fecha_inicio=n.fecha_inicio and h.id=n.id and h.precio=n.precio  
WHERE h.hora_actualizacion IS NULL;

UPDATE h
SET h.hora_actualizacion = n.hora_actualizacion
FROM Estudios.dbo.DieselHist as h
JOIN Estudios.dbo.DieselPaso as n ON h.fecha_inicio=n.fecha_inicio and h.id=n.id and h.precio=n.precio  
WHERE h.hora_actualizacion IS NULL;


DROP TABLE Estudios.dbo.Bencina93Paso;

DROP TABLE Estudios.dbo.Bencina95Paso;

DROP TABLE Estudios.dbo.Bencina97Paso;

DROP TABLE Estudios.dbo.DieselPaso;
*/


