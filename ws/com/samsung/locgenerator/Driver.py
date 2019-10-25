from com.samsung.locgenerator.XMLReader import XMLReader
from com.samsung.locgenerator.dal.MySQLConnector import MySQLConnection
from com.samsung.locgenerator.dao.DBConfig import DBConfig


def find_foreignkeys():
    


if __name__ == '__main__':
    xml_reader = XMLReader("/Users/vamsi/Edu/M.Tech/DS603/LatticeOfCuboidsGenerator/config.xml")
    print("Parsing XML")
    db_config, facts, dimensions = xml_reader.parseXML()
    print(db_config, facts, dimensions)
    db_connection = MySQLConnection.getInstance(db_config=db_config)

