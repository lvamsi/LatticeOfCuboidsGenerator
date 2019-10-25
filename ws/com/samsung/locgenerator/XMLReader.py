import xml.etree.ElementTree as ET

from com.samsung.locgenerator.dao.DBConfig import DBConfig


class XMLReader():
    def __init__(self, xml_path):
        self.xml_path = xml_path

    def __extract_dbconfig(self, root):
        dbconfig_el = root.find("DBConfig")
        db_config = {config_el.tag: config_el.text for config_el in dbconfig_el}
        print(db_config)
        db_config = DBConfig(config=db_config)
        return db_config

    def __extract_facts(self, root):
        facts_el = root.find("tables/facts")
        facts = [str(fact_el.text).strip() for fact_el in facts_el]
        return facts

    def __extract_dimensions(self, root):
        dimensions_el = root.find("tables/dimensions")
        dimensions = []
        for dimension_el in dimensions_el:
            dot_position = dimension_el.text.find(".")
            dimension = dimension_el.text if dot_position == -1 else dimension_el.text[:dot_position]
            dimensions.append(dimension)
        return dimensions

    def parseXML(self):
        tree = ET.parse(self.xml_path)
        root = tree.getroot()
        db_config = self.__extract_dbconfig(root)
        facts = self.__extract_facts(root=root)
        dimensions = self.__extract_dimensions(root=root)
        return db_config, facts, dimensions
