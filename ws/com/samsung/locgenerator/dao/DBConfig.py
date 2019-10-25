class DBConfig():
    def __init__(self,config:dict):
        self.__host = config["host"]
        self.__username = config["username"]
        self.__password = config["password"]
        self.__schema = config["schema"]
        self.__config_dict = config

    def get_host(self) -> str:
        return self.__host

    def get_username(self)->str:
        return  self.__username

    def get_password(self)->str:
        return self.__password

    def get_schema(self)->str:
        return self.__schema

    def get_config_dict(self)->dict:
        return self.__config_dict

