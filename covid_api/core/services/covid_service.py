import json
import os
from datetime import datetime, timedelta
import xlrd
import pandas as pd
from covid_api.core.models import Province
from covid_api.settings import COVID_FILE_NAME
import psycopg2

default_query = "SELECT * FROM public.covid19_casos"


class DataFrameWrapper:
    data_frame = None

    def __init__(self, data_frame):
        self.data_frame = data_frame

    def count(self):
        # Count rows
        return len(self.data_frame.index)

    def copy(self):
        return DataFrameWrapper(self.data_frame.copy())

    def filter_eq(self, column, value):
        self.data_frame = self.data_frame.loc[self.data_frame[column] == value]
        return self

    def filter_ge(self, column, value):
        self.data_frame = self.data_frame.loc[self.data_frame[column] >= value]
        return self

    def filter_le(self, column, value):
        self.data_frame = self.data_frame.loc[self.data_frame[column] <= value]
        return self

    def group_by(self, columns):
        self.data_frame = self.data_frame.groupby(columns)
        return self

    def size(self):
        self.data_frame = self.data_frame.size()
        return self

    def summary(self):
        self.data_frame = self.data_frame.describe()
        return self

    def to_json(self, orient="table"):
        df = self.data_frame.reset_index(drop=True)
        json_string = df.to_json(orient=orient)
        return json.loads(json_string)['data']

    def __getitem__(self, column):
        return self.data_frame[column]


class CovidService:

    _raw_data = None

    data_url = 'https://sisa.msal.gov.ar/datos/descargas/covid-19/files/Covid19Casos.csv'

    # Refresh time in hours
    refresh_rate = 1

    last_refresh = None

    @classmethod
    def get_data(cls, sql_query = None) -> DataFrameWrapper:
        is_time_to_refresh = True

        con = psycopg2.connect(
            host="localhost",
            database="testdb",
            user="postgres",
            password="postgres")

        cur = con.cursor()
        cur.execute('''CREATE TABLE STUDENT
              (ADMISSION INT PRIMARY KEY     NOT NULL,
              NAME           TEXT    NOT NULL,
              AGE            INT     NOT NULL,
              COURSE        CHAR(50),
              DEPARTMENT        CHAR(50));''')
        print("Table created successfully")

        con.commit()
        con.close()


        if cls.last_refresh:
            refresh_time = cls.last_refresh + timedelta(hours=cls.refresh_rate)
            is_time_to_refresh = refresh_time < datetime.now()

        if cls._raw_data is None or is_time_to_refresh:
            if not os.path.isfile(COVID_FILE_NAME):
                # Update the data from the url and save the file
                cls.update_data()

            cls._raw_data = pd.read_csv(
                COVID_FILE_NAME,
                encoding='utf-8'
            )
            # Get the base hour
            cls.last_refresh = datetime.now().replace(
                minute=0,
                second=0,
                microsecond=0
            )

        # --- Running the query --- #
        if sql_query is None:
            sql_query = default_query

        con = psycopg2.connect(
            host="localhost",
            database="testdb",
            user="postgres",
            password="postgres")

        # cur = con.cursor()
        print("executing query: " + sql_query)
        cls._raw_data = pd.read_sql_query(sql_query, con)
        print("Query executed")

        return DataFrameWrapper(cls._raw_data)

    @classmethod
    def update_data(cls):
        data_frame = pd.read_csv(
            cls.data_url,
            encoding='utf-8'
        )
        data_frame.to_csv(COVID_FILE_NAME, index=False)
        cls._raw_data = None

    def population_per_province(cls):
        provinces_population = {}
        workbook = xlrd.open_workbook('poblacion.xls')

        for worksheet in workbook.sheets():
            split_name = worksheet.name.split('-')
            if len(split_name) < 2:
                continue
            province_slug = split_name[0]
            province_name = Province.from_slug(province_slug)
            if province_name:
                provinces_population[province_slug] = worksheet.cell(16, 1).value
                continue
            else:
                country_population = worksheet.cell(15, 1).value

        provinces_population['ARG'] = country_population

        return provinces_population

    @classmethod
    def population_summary_metrics(cls, dfw, slug):

        if slug:
            population = cls.population_per_province(cls)[slug]
        else:
            population = cls.population_per_province(cls)['ARG']

        HUNDRED_THOUSAND = 100000
        MILLION = 1000000
        df = dfw.data_frame

        # per hundred thousand
        df['casos_cada_cien_mil'] = round(df['casos'] * HUNDRED_THOUSAND/population)
        df['muertes_cada_cien_mil'] = round(df['muertes'] * HUNDRED_THOUSAND/population)
        df['casos_acum_cada_cien_mil'] = round(df['casos_acum'] * HUNDRED_THOUSAND/population)
        df['muertes_acum_cada_cien_mil'] = round(df['muertes_acum'] * HUNDRED_THOUSAND/population)

        # per million
        df['casos_por_millón'] = round(df['casos'] * MILLION / population)
        df['muertes_por_millón'] = round(df['muertes'] * MILLION  / population)
        df['casos_acum_por_millón'] = round(df['casos_acum'] * MILLION  / population)
        df['muertes_acum_por_millón'] = round(df['muertes_acum'] * MILLION  / population)

        return DataFrameWrapper(df)


    @classmethod
    def summary(cls, group_by_vector, start_date, end_date, data):

        start_date = start_date if start_date else '2020-02-11'

        if not end_date:
            end_date = CovidService.get_data()['ultima_actualizacion'].max()

        summary = data.data_frame

        raw_range = pd.date_range(start=start_date, end=end_date)
        range_strings = raw_range.format(formatter=lambda x: x.strftime('%Y-%m-%d'))
        df = pd.DataFrame(range_strings, columns=['fecha_diagnostico'])

        cases_count = summary.groupby(
            [elem for elem in group_by_vector] + ['fecha_diagnostico'],
            as_index=False
        ).count()
        df2 = cases_count[['fecha_diagnostico']].copy()
        df2['casos'] = cases_count['id_evento_caso']

        summary = summary.loc[summary['fallecido'] == 'SI']
        deaths_count = summary.groupby(
            [elem for elem in group_by_vector] + ['fecha_fallecimiento'],
            as_index=False
        ).count()[['fecha_fallecimiento', 'id_evento_caso']]
        deaths_count = deaths_count.rename(
            columns={'id_evento_caso': "muertes", 'fecha_fallecimiento': "fecha_diagnostico"})

        df = df.merge(df2, on='fecha_diagnostico', how='left')
        df = df.merge(deaths_count, on='fecha_diagnostico', how='left')

        df = df.fillna(value=0)

        df['muertes_acum'] = df['muertes'].cumsum()
        df['casos_acum'] = df['casos'].cumsum()

        df = df.rename(
            columns={'fecha_diagnostico': "fecha"})

        return DataFrameWrapper(df)


    @classmethod
    def execute_query(cls, query) -> DataFrameWrapper:
        con = psycopg2.connect(
            host="localhost",
            database="testdb",
            user="postgres",
            password="postgres")

        # cur = con.cursor()
        print("executing query: " + query)
        # date = cur.execute(query)
        dat = pd.read_sql_query(query, con)
        print("Query executed")

        return dat


    @classmethod
    def grouper(cls, iterable, n, fillvalue=None):
        args = [iter(iterable)] * n
        return itertools.zip_longest(*args, fillvalue=fillvalue)


    @classmethod
    def read_csv_chunks(cls, csv_url, chunk_size):
        with contextlib.closing(requests.get(csv_url, stream=True)) as stream:
            lines = (line.decode('utf-8') for line in stream.iter_lines(chunk_size))
            reader = csv.reader(lines, delimiter=',', quotechar='"')
            chunk = grouper(reader, chunk_size, None)
            while True:
                try:
                    yield [line for line in next(chunk)]
                except StopIteration:
                    return


    @classmethod
    def dump_csv_lines_into_db(cls, connection, csv_lines, csv_url, chunk_size):
        table_name = "tabla"
        columns = "columnas"
        insert_sql = """INSERT INTO """ + table_name + "(" + columns + ")" +  """VALUES(%s)"""
        cur = connection.cursor()

        for csv_lines in read_csv_chunks(csv_url, chunk_size):
            cur.execute(insert_sql, csv_lines)
            connection.commit()

        cur.close()
