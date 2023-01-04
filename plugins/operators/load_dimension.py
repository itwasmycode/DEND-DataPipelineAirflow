from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {} ({});
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query="",
                 target_table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query = query
        self.target_table = target_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_query = LoadDimensionOperator.insert_sql.format(self.target_table, self.query)
        redshift.run(insert_query)
