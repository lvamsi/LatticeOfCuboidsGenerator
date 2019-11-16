from com.samsung.locgenerator.XMLReader import XMLReader
from com.samsung.locgenerator.dal.MySQLConnector import MySQLConnection
from com.samsung.locgenerator.dal.QueryTemplates import QueryTemplates
from com.samsung.locgenerator.dao.Table import Table


class Column:
    def __init__(self, name: str, table: Table):
        self.name = name
        self.table = table
        self.column_type = None
        self.is_foreign_key = False

    def set_name(self, name: str):
        self.name = name

    def set_type(self, data_type: str):
        self.column_type = data_type

    def extract_data_type(self, db_connection: MySQLConnection, schema):
        query = QueryTemplates.GET_COLUMNS.format(schema=schema, table_name=self.table.name)
        cursor = db_connection.cursor(dictionary=True)
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            self.column_type = row['COLUMN_TYPE']

    def set_as_foreign_key(self, referenced_table: Table, referenced_column):
        self.is_foreign_key = True
        self.referenced_table = referenced_table
        self.referenced_column = referenced_column


# Test
if __name__ == '__main__':
    xml_reader = XMLReader("/Users/vamsi/Edu/M.Tech/DS603/LatticeOfCuboidsGenerator/config.xml")
    db_config, facts, dimensions, aggregations = xml_reader.parseXML()
    db_connection = MySQLConnection.getInstance(db_config=db_config.get_config_dict())

    table = Table("f_market")
    col = Column("Ord_id", table)
    col.extract_data_type(db_connection, 'sales')
    print(col.column_type)
