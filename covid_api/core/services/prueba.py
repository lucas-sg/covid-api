import contextlib
import itertools
import requests
import csv

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

data_url = 'https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Casos.csv'

for chunk in read_csv_chunks(data_url, 100):
    values = '{},"{}",{},"{}","{}","{}","{}","{}","{}","{}",{},"{}","{}","{}","{}","{}","{}",{},"{}","{}","{}",{},"{}",{},"{}"'
    clean_chunk = [tuple(line) for line in chunk if not line is None]
    x = 0