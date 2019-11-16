from com.samsung.locgenerator.dao.Column import Column
from com.samsung.locgenerator.XMLReader import XMLReader
from com.samsung.locgenerator.dal.MySQLConnector import MySQLConnection
from com.samsung.locgenerator.dao.Table import Table


class FactColumn(Column):
    def __init__(self, name:str, table:Table, aggregation: str):
        super().__init__(name,table)
        self.aggregation = aggregation

    def get_aggregated_col_name(self):
        return self.aggregation + "_" + str(self.name)

#Test
if __name__ == '__main__':
    xml_reader = XMLReader("/Users/vamsi/Edu/M.Tech/DS603/LatticeOfCuboidsGenerator/config.xml")
    db_config, facts, dimensions, aggregations = xml_reader.parseXML()
    db_connection = MySQLConnection.getInstance(db_config=db_config.get_config_dict())

    table = Table("f_market")
    col = FactColumn("Order_Quantity", table, "sum")
    col.extract_data_type(db_connection, 'sales')
    print(col.get_aggregated_col_name())
