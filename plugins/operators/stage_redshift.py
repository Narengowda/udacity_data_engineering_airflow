from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

COPY_CMD = "COPY {} FROM '{}' ACCESS_KEY_ID '{}' SECRET_ACCESS_KEY '{}' "
        


# JSON_COPY_CMD = COPY_CMD + " JSON '{}' COMPUPDATE OFF"
JSON_COPY_CMD = COPY_CMD + "  format as JSON 'auto' region as 'us-west-2';"

CSV_COPY_CMD = COPY_CMD + " IGNOREHEADER {} DELIMITER ',' region as 'us-west-2'"

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table_name="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 load_type="",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.load_type = load_type
        self.aws_credentials_id = aws_credentials_id
        self.ignore_headers = ignore_headers
        self.log.info(f"init")
        

    def execute(self, context):
        """copies data from S3 to redshift""" 
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Copying {self.load_type} data from s3 to redshift, table: {self.table_name}")

        if self.load_type == "json":
            self.copy_json_data(context, credentials, redshift)
        else:
            self.copy_csv_data(context, credentials, redshift)

    def get_s3_path(self, context):
        key = self.s3_key.format(**context)
        return "s3://{}/{}".format(self.s3_bucket, key) 

    def copy_json_data(self, context, credentials, redshift):
        """copies json data from S3 to redshift""" 
        formatted_sql = JSON_COPY_CMD.format(
                self.table_name,
                self.get_s3_path(context),
                credentials.access_key,
                credentials.secret_key,
                self.json_path
            )
        self.log.info(">>>>>>>")
        self.log.info(f"{formatted_sql}")
        self.log.info(">>>>>>>")
        redshift.run(formatted_sql)

    def copy_csv_data(self, context, credentials, redshift):
        """copies csv data from S3 to redshift""" 
        formatted_sql = CSV_COPY_CMD.format(
                self.table_name,
                self.get_s3_path(context),
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers
            )
        redshift.run(formatted_sql)
