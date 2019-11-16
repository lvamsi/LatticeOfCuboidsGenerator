from com.samsung.locgenerator.XMLReader import XMLReader
from com.samsung.locgenerator.dal.MySQLConnector import MySQLConnection
from com.samsung.locgenerator.dal.QueryTemplates import QueryTemplates
from itertools import combinations


def find_combinations(dimensions):
    dim_combinations = []
    for r in range(1, len(dimensions)):
        dim_combinations += list(combinations(dimensions, r))
    return dim_combinations


def get_foreign_keys(db_connection, db_config: dict, facts, dimensions):
    cursor = db_connection.cursor(dictionary=True)
    foreign_keys = {}
    for fact in facts:
        foreign_keys[fact] = {}
        for dimension in dimensions:
            query = QueryTemplates.GET_FOREIGN_KEYS.format(schema=db_config['schema'], referencing_table=fact, referenced_table=dimension)
            cursor.execute(query)
            for row in cursor:
                foreign_keys[fact][dimension] = row['REFERENCED_COLUMN_NAME']
    return foreign_keys


def find_column_types(db_connection, db_config: dict, facts):
    cursor = db_connection.cursor(dictionary=True)
    column_types = {}
    columns = {}
    for fact in facts:
        query = QueryTemplates.GET_COLUMNS.format(schema=db_config['schema'], table_name=fact)
        cursor.execute(query)
        rows = cursor.fetchall()
        column_types[fact] = {row['COLUMN_NAME']: row['COLUMN_TYPE'] for row in rows}
        columns[fact] = [row['COLUMN_NAME'] for row in rows]
    return columns, column_types


def get_column_type( db_connection: MySQLConnection, schema, table_name):
    query = QueryTemplates.GET_COLUMNS.format(schema=schema, table_name=table_name)
    cursor = db_connection.cursor(dictionary=True)
    cursor.execute(query)
    rows = cursor.fetchall()
    for row in rows:
        return row['COLUMN_TYPE']



def create_tables(db_connection, db_config_dict: dict, facts, dimensions, foreign_keys, columns, column_types, combinations):
    cursor = db_connection.cursor(dictionary=True)
    aggregations = ['sum']
    queries = []
    insert_queries = []
    for fact in facts:
        for combination in combinations:
            table_name = "lat_" + fact + "_" + "_".join(combination)
            pk_column_defn = "`{table_name}_id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY".format(table_name=table_name)
            fk_column_defns = ",".join([QueryTemplates.CREATE_TABLE['COLUMN_DEFINITION'].format(column_name=dimensions[dim_table_name] if dimensions[dim_table_name] is not None else foreign_keys[fact][dim_table_name], data_type=  get_column_type(db_connection,db_config_dict['schema'],dim_table_name)    if dimensions[dim_table_name] is not None else column_types[fact][foreign_keys[fact][dim_table_name]]) for dim_table_name in combination])
            fact_cols = set(columns[fact]).difference(foreign_keys[fact].values())
            fact_col_templates = []
            aggregated_fact_col_names = []
            agg_fact_col_names = []
            for fact_col in fact_cols:
                for aggregation in aggregations:
                    column_name = aggregation + "_" + fact_col
                    agg_fact_col_names.append(column_name)
                    aggregated_fact_col_names.append(("{aggregation_function}({fact_table_name}.{fact_col}) as {column_name}").format(aggregation_function=aggregation,fact_table_name=fact,fact_col=fact_col,column_name=column_name))
                    fact_col_templates.append(
                        QueryTemplates.CREATE_TABLE['COLUMN_DEFINITION'].format(column_name=column_name, data_type=column_types[fact][fact_col]))

            agg_fact_columns_definitions = ",".join(fact_col_templates)
            query = QueryTemplates.CREATE_TABLE['CREATE_QUEY'].format(table_name=table_name, pk_col_defn=pk_column_defn, fk_col_defn=fk_column_defns, agg_fact_col_defn=agg_fact_columns_definitions)
            queries.append(QueryTemplates.DROP_TABLE.format(table_name=table_name))
            queries.append(query)
            # Insert Data
            dim_col_names = []
            dime_col_names = []
            join_conditions = []

            for dim_table_name in combination:
                dim_col_names.append(dim_table_name+"."+dimensions[dim_table_name] if dimensions[dim_table_name] is not None else fact+"."+foreign_keys[fact][dim_table_name])
                dime_col_names.append(dimensions[dim_table_name] if dimensions[dim_table_name] is not None else foreign_keys[fact][dim_table_name])
                #sales.d_customer ON sales.f_market.Cust_id = sales.d_customer.Cust_id
                join_condition = ("{dim_table_name} on {fact_table_name}.{fk} = {dim_table_name}.{fk}").format(dim_table_name=dim_table_name,fact_table_name = fact,fk=foreign_keys[fact][dim_table_name])
                join_conditions.append(join_condition)
            select_query = ("select {dim_col_names},{aggregated_fact_col_names} from {fact_table_name} JOIN {join_conditions} group by {dim_col_names};").format(dim_col_names=", ".join(dim_col_names),join_conditions=" JOIN ".join(join_conditions),fact_table_name=fact,aggregated_fact_col_names= ", ".join(aggregated_fact_col_names))
            # print("_____________________Col Names___________")
            # print(dim_col_names)
            # print("__________Joins___________")
            # print(join_conditions)
            # print("______________select query___________")
            # print(select_query)
            # print(table_name)
            # print(",".join(dime_col_names+agg_fact_col_names))
            insert_query = ("INSERT into {table_name}({cols}) {select_query}").format(table_name=table_name,cols=",".join(dime_col_names+agg_fact_col_names),select_query=select_query )
            insert_queries.append(insert_query)


    for query in queries:
        cursor.execute(query)
    for insert_query in insert_queries:
        try:
            cursor.execute(insert_query)
        except:
            print(insert_query)

    db_connection.commit()


if __name__ == '__main__':
    xml_reader = XMLReader("/Users/vamsi/Edu/M.Tech/DS603/LatticeOfCuboidsGenerator/config.xml")
    print("[Start] Parsing XML")
    db_config, facts, dimensions, aggregations = xml_reader.parseXML()
    print(db_config, facts, dimensions, aggregations)
    print("[Done] Parsing XML")
    print("[Start] Establishing DB Connection")
    db_connection = MySQLConnection.getInstance(db_config=db_config.get_config_dict())
    print(db_connection)
    print("[End] Establishing DB Connection")
    foreign_keys = get_foreign_keys(db_connection, db_config.get_config_dict(), facts, dimensions)
    print("Foreign Keys")
    print(foreign_keys)
    dimension_combincations = find_combinations(dimensions)
    print("Combinations")
    print(dimension_combincations)
    columns, column_types = find_column_types(db_connection, db_config.get_config_dict(), facts)
    print(column_types)
    create_tables(db_connection, db_config.get_config_dict(), facts, dimensions, foreign_keys, columns, column_types, dimension_combincations)
