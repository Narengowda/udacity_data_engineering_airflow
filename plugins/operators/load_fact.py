import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 query="",
                 mode="truncate",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.query = query
        self.truncate = True if mode == "truncate" else False

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"loading tables>> {self.table_name}")
        truncate_insert_sql = f"TRUNCATE TABLE {self.table_name};INSERT INTO {self.table_name}{self.query}; COMMIT;"
        sql = f"INSERT INTO {self.table_name}{self.query}; COMMIT;"
        
        if self.truncate:
            redshift.run(truncate_insert_sql)
        else:
            redshift.run(sql)