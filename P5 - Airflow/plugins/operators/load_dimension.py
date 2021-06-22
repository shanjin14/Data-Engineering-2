from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query="",
                 append_data =False,
                 table_name ="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.append_data = append_data
        self.table_name = table_name

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Connect to RedShift")
        if self.append_data:
            redshift.run(self.query)
            self.log.info('Run Append Load %s', self.query)
        else:
            truncate_statement = 'TRUNCATE %s' % self.table_name
            redshift.run(truncate_statement)
            redshift.run(self.query)
            self.log.info('Run Truncate and Load %s %s',truncate_statement, self.query)

