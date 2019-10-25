class QueryTemplates:
    GET_FOREIGN_KEYS = (
        "SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE CONSTRAINT_SCHEMA = '{schema}' AND TABLE_NAME = '{referencing_table}' AND REFERENCED_TABLE_NAME = '{referenced_table}';")
    GET_COLUMNS = (
    "SELECT * FROM information_schema.columns WHERE table_schema='{schema}' AND table_name='{table_name}';");
    CREATE_TABLE = {
        'COLUMN_DEFINITION': "`{column_name}` {data_type} DEFAULT NULL",
        'CREATE_QUEY': "CREATE TABLE `{table_name}` ( {pk_col_defn} , {fk_col_defn} ,{agg_fact_col_defn}) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
    }
