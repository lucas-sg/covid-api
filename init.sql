CREATE TABLE public.covid19_casos (
    id_evento_caso int PRIMARY KEY,
    sexo varchar(2),
    edad int,
    edad_años_meses varchar(6),
    residencia_pais_nombre varchar(100),
    residencia_provincia_nombre varchar(100),
    residencia_departamento_nombre varchar(100),
    carga_provincia_nombre varchar(100),
    fecha_inicio_sintomas date,
    fecha_apertura date,
    sepi_apertura int,
    fecha_internacion date,
    cuidado_intensivo varchar(30),
    fecha_cui_intensivo date,
    fallecido varchar(30),
    fecha_fallecimiento date,
    asistencia_respiratoria_mecanica varchar(30),
    carga_provincia_id int,
    origen_financiamiento varchar(100),
    clasificacion varchar(100),
    clasificacion_resumen varchar(100),
    residencia_provincia_id int,
    fecha_diagnostico date,
    residencia_departamento_id int,
    ultima_actualizacion date
);