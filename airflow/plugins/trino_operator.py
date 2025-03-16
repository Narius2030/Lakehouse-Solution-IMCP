from airflow.models.baseoperator import BaseOperator            # type: ignore
from airflow.utils.decorators import apply_defaults             # type: ignore
from airflow.providers.trino.hooks.trino import TrinoHook       # type: ignore
import logging
import os
from typing import Sequence, Callable, Optional

def handler(cur):
    cur.fetchall()

class TrinoCustomHook(TrinoHook):

    def run(
        self,
        sql,
        autocommit: bool = False,
        parameters: Optional[dict] = None,
        handler: Optional[Callable] = None,
    ) -> None:
        """:sphinx-autoapi-skip:"""

        return super(TrinoHook, self).run(
            sql=sql, autocommit=autocommit, parameters=parameters, handler=handler
        )

class TrinoOperator(BaseOperator):
    template_fields: Sequence[str] = ('sql',)
    template_ext = ('.sql',)

    @apply_defaults
    def __init__(
        self, trino_conn_id: str, 
        sql, parameters=None, 
        do_xcom_push: bool = False, 
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.trino_conn_id = trino_conn_id
        self.sql = sql
        self.parameters = parameters
        self.do_xcom_push = do_xcom_push

    def execute(self, context):
        task_instance = context['task']

        logging.info('Creating Trino connection')
        hook = TrinoCustomHook(trino_conn_id=self.trino_conn_id)

        # Kiểm tra nếu sql là file, đọc nội dung file trước khi render
        if isinstance(self.sql, str) and self.sql.endswith('.sql'):
            if not os.path.exists(self.sql):
                raise FileNotFoundError(f"SQL file not found: {self.sql}")

            with open(self.sql, 'r', encoding='utf-8') as f:
                raw_sql = f.read()
        else:
            raw_sql = self.sql  # Nếu không phải file, giữ nguyên SQL

        # Render SQL từ Jinja template (nếu có)
        sql_statements = self.render_template(raw_sql, context)

        if isinstance(sql_statements, str):
            sql = list(filter(None,sql_statements.strip().split(';')))

            if len(sql) == 1:
                logging.info('Executing single sql statement')
                sql = sql[0]
                result = hook.get_first(sql, parameters=self.parameters)
                logging.info(f'SQL Result: {result}')
                if self.do_xcom_push:
                    return result
                return None

            if len(sql) > 1:
                logging.info('Executing multiple sql statements')
                result = hook.get_first(sql, parameters=self.parameters)
                logging.info(f'SQL Result: {result}')
                if self.do_xcom_push:
                    return result
                return None

        if isinstance(sql_statements, list):
            sql = []
            for sql_statement in sql_statements:
                sql.extend(list(filter(None,sql_statement.strip().split(';'))))

            logging.info('Executing multiple sql statements')
            return hook.run(sql, autocommit=False, parameters=self.parameters, handler=handler)