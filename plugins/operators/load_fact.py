from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_sql = """
        INSERT INTO {} ({});
    """
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query="",
                 target_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.query = query
        self.target_table = target_table

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        insert_query = LoadFactOperator.insert_sql.format(self.target_table, self.query)

        redshift.run(insert_query)
