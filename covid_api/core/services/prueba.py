import contextlib
import itertools
import requests
import csv
import psycopg2


def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args, fillvalue=fillvalue)


def read_csv_chunks(csv_url, chunk_size):
    with contextlib.closing(requests.get(csv_url, stream=True)) as stream:
        lines = (line.decode('utf-8') for line in stream.iter_lines(chunk_size))
        reader = csv.reader(lines, delimiter=',', quotechar='"')
        chunk = grouper(reader, chunk_size, None)
        while True:
            try:
                yield [line for line in next(chunk)]
            except StopIteration:
                return

def replace_value_tuple(idx, tup, value):
    aux = list(tup)
    aux[idx] = value
    return tuple(aux)

def dump_csv_lines_into_db(connection, csv_url, chunk_size):
    columns = """id_evento_caso,sexo,edad,edad_años_meses,residencia_pais_nombre,residencia_provincia_nombre,residencia_departamento_nombre,carga_provincia_nombre,fecha_inicio_sintomas,fecha_apertura,sepi_apertura,fecha_internacion,cuidado_intensivo,fecha_cui_intensivo,fallecido,fecha_fallecimiento,asistencia_respiratoria_mecanica,carga_provincia_id,origen_financiamiento,clasificacion,clasificacion_resumen,residencia_provincia_id,fecha_diagnostico,residencia_departamento_id,ultima_actualizacion"""
    insert_sql = 'INSERT INTO public.covid19_casos (' + columns + ') VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
    cur = connection.cursor()

    i = 0
    for csv_chunk in read_csv_chunks(csv_url, chunk_size):
        if i == 0:
            clean_chunk = [tuple(line) for line in csv_chunk[1:] if not line is None and len(line)==25]
        else:
            clean_chunk = [tuple(line) for line in csv_chunk if not line is None and len(line)==25]

        i = 1

        for index, tup in enumerate(clean_chunk):
            if tup[8] == '':
                clean_chunk[index] = replace_value_tuple(8, clean_chunk[index], None)
            if tup[9] == '':
                clean_chunk[index] = replace_value_tuple(9, clean_chunk[index], None)
            if tup[11] == '':
                clean_chunk[index] = replace_value_tuple(11, clean_chunk[index], None)
            if tup[13] == '':
                clean_chunk[index] = replace_value_tuple(13, clean_chunk[index], None)
            if tup[15] == '':
                clean_chunk[index] = replace_value_tuple(15, clean_chunk[index], None)
            if tup[22] == '':
                clean_chunk[index] = replace_value_tuple(22, clean_chunk[index], None)
            if tup[24] == '':
                clean_chunk[index] = replace_value_tuple(24, clean_chunk[index], None)

        cur.executemany(insert_sql, clean_chunk)
        connection.commit()

    cur.close()

con = psycopg2.connect(
            host="localhost",
            database="testdb",
            user="postgres",
            password="postgres")

data_url = 'https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Casos.csv'

dump_csv_lines_into_db(con, data_url, 10)