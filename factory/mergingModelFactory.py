import sys
parentPath='c:/Users/user/PycharmProjects/personnelInfo' # parent 경로
sys.path.append(parentPath) # 경로 추가
import pandas as pd
from datetime import datetime
from config import config
from util import util
from factory import simpleModelFactory, advancedModelFactory

class DbAtTime:
    def __init__(self, date):
        self.date=date
        self.dfs = []   
        self.compose_df()
        self.df = self.get_df()
    
    def register_df(self, df):
        self.dfs.append(df)
    
    def compose_df(self):
        df = simpleModelFactory.UniqueInfoDf().df
        con1 = df.loc[:, 'resignationDate'] >= self.date
        con2 = df.loc[:, 'resignationDate'].isna()
        con3 = df.loc[:, 'recruitmentDate'] <= self.date
        df = df.loc[(con1|con2) & con3]
        self.register_df(df)
        self.register_df(advancedModelFactory.DepartmentPositionLevelDf(self.date).df)
        self.register_df(advancedModelFactory.ContactState(self.date).df)
        self.register_df(simpleModelFactory.ContactInfoDf().df)

    def get_df(self):
        df = self.dfs[0]
        for i in self.dfs[1:]:
            df = pd.merge(df, i, how='left', on='id_')
        df=df.sort_values(by=['recruitmentDate', 'birth'])
        df=df.sort_values(by=['appoPosLevel_to_today'], ascending=False)
        df=df.sort_values(by=['depart_order', 'level_order', 'appoLeader'])
        df = df.reset_index()
        return df

class Resignation:
    def __init__(self, date):
        self.date=date
        self.dfs = []   
        self.compose_df()
        self.df = self.get_df()
    
    def register_df(self, df):
        self.dfs.append(df)
    
    def compose_df(self):
        df = simpleModelFactory.UniqueInfoDf().df
        con1 = df.loc[:, 'resignationDate'] <= self.date
        df = df.loc[(con1)]
        self.register_df(df)
        self.register_df(advancedModelFactory.DepartmentPositionLevelDf(self.date).df)
        self.register_df(advancedModelFactory.ContactState(self.date).df)

    def get_df(self):
        df = self.dfs[0]
        for i in self.dfs[1:]:
            df = pd.merge(df, i, how='left', on='id_')
        df=df.sort_values(by=['recruitmentDate', 'birth'])
        df=df.sort_values(by=['appoPosLevel_to_today'], ascending=False)
        df=df.sort_values(by=['depart_order', 'level_order', 'appoLeader'])
        df = df.reset_index(drop=True)
        return df

date = datetime(2022,4,27)
df = DbAtTime(date).df

df.to_csv('DbAtTime.csv', encoding='cp949')
print(df)