class MySQLConnector:
    __instance = None

   @staticmethod
   def getInstance(db_config:dict):
      if MySQLConnector.__instance == None:
          MySQLConnector()
      return MySQLConnector.__instance

   def __init__(self):
      if MySQLConnector.__instance != None:
         raise Exception("This class is a singleton!")
      else:
          MySQLConnector.__instance = self

