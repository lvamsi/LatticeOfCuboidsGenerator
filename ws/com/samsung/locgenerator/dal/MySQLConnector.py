import mysql.connector


class MySQLConnection:
    __inst = None

    @staticmethod
    def getInstance(db_config: dict):
        if MySQLConnection.__inst is None:
            return mysql.connector.connect(host=db_config["host"], user=db_config["username"],
                                           password=db_config["password"], database=db_config["schema"])
        else:
            return MySQLConnection.__instance
