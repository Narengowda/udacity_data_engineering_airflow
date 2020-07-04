import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_names=(),
                 test_data=(),
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table_names = table_names
        self.redshift_conn_id = redshift_conn_id
        self.test_data=test_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test_data in self.test_data:
            entries = redshift.get_records(test_data['query'])
            
            if len(entries[0]) == 0:
                raise Exception(f"data quality check error. {test_data['query']} returned 0 entries ")
            
            if entries[0][0] < test_data['expected_count']:
                raise Exception(f"data quality check error. {test_data['query']} returned {entries[0][0]} entries")

            self.log.info(f"data quality check on {test_data['query']} is ok with {entries[0][0]} entreis")