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
            query = QueryTemplates.GET_FOREIGN_KEYS.format(schema=db_config['schema'], referencing_table=fact,
                                                           referenced_table=dimension)
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


def create_tables(db_connection, db_config: dict, facts, dimensions, foreign_keys, columns, column_types, combinations):
    cursor = db_connection.cursor(dictionary=True)
    aggregations = ['sum']
    queries = []
    for fact in facts:
        for combination in combinations:
            table_name = "lat_" + fact + "_" + "_".join(combination)
            pk_column_defn = "`{table_name}_id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY".format(table_name=table_name)
            fk_column_defns = ",".join([QueryTemplates.CREATE_TABLE['COLUMN_DEFINITION'].format(column_name=column_name,
                                                                                                data_type=
                                                                                                column_types[fact][
                                                                                                    foreign_keys[fact][
                                                                                                        column_name]])
                                        for
                                        column_name in combination])
            fact_cols = set(columns[fact]).difference(foreign_keys[fact].values())
            fact_col_templates = []
            for fact_col in fact_cols:
                for aggregation in aggregations:
                    column_name = aggregation + "-" + fact_col
                    fact_col_templates.append(
                        QueryTemplates.CREATE_TABLE['COLUMN_DEFINITION'].format(column_name=column_name,
                                                                                data_type=column_types[fact][fact_col]))
            agg_fact_columns_definitions = ",".join(fact_col_templates)
            query = QueryTemplates.CREATE_TABLE['CREATE_QUEY'].format(table_name=table_name, pk_col_defn=pk_column_defn,
                                                                      fk_col_defn=fk_column_defns,
                                                                      agg_fact_col_defn=agg_fact_columns_definitions)
            queries.append(query)
    for query in queries:
        cursor.execute(query)
    db_connection.commit()


if __name__ == '__main__':
    xml_reader = XMLReader("/Users/vamsi/Edu/M.Tech/DS603/LatticeOfCuboidsGenerator/config.xml")
    print("Parsing XML")
    db_config, facts, dimensions = xml_reader.parseXML()
    print(db_config, facts, dimensions)
    db_connection = MySQLConnection.getInstance(db_config=db_config.get_config_dict())
    print(db_connection)
    foreign_keys = get_foreign_keys(db_connection, db_config.get_config_dict(), facts, dimensions)
    print("Foreign Keys")
    print(foreign_keys)
    dimension_combincations = find_combinations(dimensions)
    print("Combinations")
    print(dimension_combincations)
    columns, column_types = find_column_types(db_connection, db_config.get_config_dict(), facts)
    print(column_types)
    create_tables(db_connection, db_config.get_config_dict(), facts, dimensions, foreign_keys, columns, column_types,
                  dimension_combincations)
