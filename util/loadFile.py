import sys
parentPath='c:/Users/user/PycharmProjects/personnelInfo' # parent 경로
sys.path.append(parentPath) # 경로 추가
from config import config

from typing_extensions import Protocol
import pandas as pd

class ILoader(Protocol):

    def load(self):
        ...

class CsvToPandas:

    def __init__(self, fileName, encodingType):
        self.fileName = fileName
        self.encodingType = encodingType

    def load(self):
        filePath = config.get_file_path(self.fileName)
        return pd.read_csv(filePath, encoding=self.encodingType)
    

class LoadFactory:

    def get_loader(self, loader:str, **kwagrs) -> ILoader:
        _dict = {'csvToPandas':CsvToPandas(kwagrs['fileName'], kwagrs['enconding'])}
        return _dict.get(loader)