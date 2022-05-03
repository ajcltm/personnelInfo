import sys
parentPath='c:/Users/user/PycharmProjects/personnelInfo' # parent 경로
sys.path.append(parentPath) # 경로 추가
from abc import ABC, abstractclassmethod
import pandas as pd
from datetime import datetime
from config import config
from util import util
from factory import simpleModelFactory, advancedModelFactory

class IIdModels(ABC):

    @abstractclassmethod
    def get_df(self):
        pass

    @abstractclassmethod
    def merge(self):
        pass


class IdModels(IIdModels):

    def __init__(self, base_df, dfs_list):
        self.base_df = base_df
        self.dfs_list = dfs_list

    def get_df(self):
        self.df = self.merge()
        return self.df

    def merge(self):
        df = self.base_df
        for i in self.dfs_list:
            df = pd.merge(df, i, how='left', on='id_')
        return df

class EmployeeListFactory:

    def __init__(self, date) :
        self.date=date

    def get_df(self) -> IIdModels:
        base_df = self.get_base_df()
        df1 = advancedModelFactory.DepartmentPositionLevelDf(self.date).df
        df2 = advancedModelFactory.ContactState(self.date).df
        dfs_list = [df1, df2]
        df = IdModels(base_df, dfs_list).get_df()
        df = self.process(df)
        return df

    def get_base_df(self):
        df = simpleModelFactory.UniqueInfoDf().df
        con1 = df.loc[:, 'resignationDate'] >= self.date
        con2 = df.loc[:, 'resignationDate'].isna()
        con3 = df.loc[:, 'recruitmentDate'] <= self.date
        df = df.loc[(con1|con2) & con3]
        return df

    def process(self, df):
        df=df.sort_values(by=['recruitmentDate', 'birth'])
        df=df.sort_values(by=['appoPosLevel_to_today'], ascending=False)
        df=df.sort_values(by=['depart_order', 'level_order', 'appoLeader'])
        df = df.reset_index()
        return df

class ResignationFactory:

    def __init__(self, date) :
        self.date=date

    def get_df(self) -> IIdModels:
        base_df = self.get_base_df()
        df1 = advancedModelFactory.DepartmentPositionLevelDf(self.date).df
        df2 = advancedModelFactory.ContactState(self.date).df
        dfs_list = [df1, df2]
        df = IdModels(base_df, dfs_list).get_df()
        df = self.process(df)
        return df

    def get_base_df(self):
        df = simpleModelFactory.UniqueInfoDf().df
        con1 = df.loc[:, 'resignationDate'] <= self.date
        df = df.loc[(con1)]
        return df

    def process(self, df):
        df=df.sort_values(by=['recruitmentDate', 'birth'])
        df=df.sort_values(by=['appoPosLevel_to_today'], ascending=False)
        df=df.sort_values(by=['depart_order', 'level_order', 'appoLeader'])
        df = df.reset_index(drop=True)
        return df

class DataProvider:
    __dict = {
        '사원명부' : EmployeeListFactory,
        '퇴직명부' : ResignationFactory
    }
    
    def get_data(self, data, date):
        __callable = self.__dict.get(data)
        df = __callable(date).get_df()
        return df



date = datetime(2022,4,27)
# df = DbAtTime(date).df

# df.to_csv('DbAtTime.csv', encoding='cp949')
# print(df)

df = DataProvider().get_data('퇴직명부', date)
print(df)