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

    def __init__(self, base_df, dfs_list, on='id_'):
        self.base_df = base_df
        self.dfs_list = dfs_list
        self.on = on

    def get_df(self):
        self.df = self.merge()
        return self.df

    def merge(self):
        df = self.base_df
        for i in self.dfs_list:
            df = pd.merge(df, i, how='left', on=self.on)
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
        df = df.loc[~(df.loc[:,'appoPositionLevel']=='위원장')]
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


class EmployeeMajorHistory:

    def __init__(self, date) :
        self.date=date

    def get_df(self) -> IIdModels:
        base_df = self.get_base_df()
        df1 = advancedModelFactory.DepartmentPositionLevelDf(self.date).df
        df2 = advancedModelFactory.AppointingHistoryDf(self.date).df
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

class DepartmentComposition:

    def __init__(self, date) :
        self.date=date

    def get_df(self) -> IIdModels:
        employeeListDf = DataProvider().get_data('사원명부', self.date)
        compo_dict = self.get_compo(employeeListDf)
        compo_dict_string = self.get_compo_str_dict(compo_dict)
        df = pd.DataFrame(list(compo_dict_string.values()), index=pd.Series(compo_dict_string.keys(), name='appoDepartment'))
        df = df.reset_index()
        df1 = self.get_depart_count_regular(employeeListDf)
        df2 = self.get_depart_count_temprory(employeeListDf)
        print(df2)
        df3 = self.get_depart_count_total(employeeListDf)
        dfs_list = [df1, df2, df3]
        df = IdModels(df, dfs_list, on='appoDepartment').get_df()
        df = self.process(df)
        return df

    def get_compo(self, df):
        container_dict = {}
        __dict = df.to_dict(orient='records')
        for i in __dict:
            department = i['appoDepartment']
            person = i['name']
            level = i['appoPositionLevel']
            contract = i['contact_state']
            if not department in container_dict:
                container_dict[department] = {
                    '사무총장':[], '운영국장':[], '본부장':[], 
                    '1급':[], '2급':[], '3급':[], '4급':[], '5급':[], 
                    '계약직':[], '전문사무직 가급':[], '전문사무직 나급':[], '전문사무직 다급':[],
                    '파견5급':[]
                    }
            if level == '5급' and contract=='정규직':
                container_dict[department]['5급'].append(person)
            elif level == '5급' and contract=='일반사무직':
                container_dict[department]['계약직'].append(person)
            elif level == '위원장':
                container_dict[department]['계약직'].append(person)
            else:
                container_dict[department][level].append(person)
        return container_dict

    def get_compo_str_dict(self, compo_dict):
        container_dict=compo_dict
        for key, value in compo_dict.items():
            for key_, value_ in value.items():
                length = len(value_)
                if length > 0:
                    compo_string = '\n'.join(value_)
                    string = f'({length})\n{compo_string}'
                    container_dict[key][key_] = string
                else:
                    container_dict[key][key_] = "-"
        return container_dict

    def get_depart_count_regular(self, df):
        df = df.loc[df.loc[:,'contact_state']=='정규직'][['appoDepartment', 'name']]
        s = df.groupby('appoDepartment').count()
        return s
    
    def get_depart_count_temprory(self, df):
        df = df.loc[~(df.loc[:,'contact_state']=='정규직')][['appoDepartment', 'name']]
        s = df.groupby('appoDepartment').count()
        return s

    def get_depart_count_total(self, df):
        df = df[['appoDepartment', 'name']]
        s = df.groupby('appoDepartment').count()
        return s

    def process(self, df):
        df.columns = [
            '부서','사무총장', '운영국장', '본부장','1급', '2급', '3급', '4급', '5급',
            '계약 5급', '전문 가급', '전문 나급', '전문 다급','파견 5급','정규직계','비정규직계', '총계']
        df = df[[
            '부서','사무총장', '운영국장', '본부장','1급', '2급', '3급', '4급', '5급','정규직계',
            '계약 5급', '전문 가급', '전문 나급', '전문 다급','파견 5급','비정규직계', '총계']]
        df = df.fillna("-")
        return df

class DataProvider:
    __dict = {
        '사원명부' : EmployeeListFactory,
        '퇴직명부' : ResignationFactory,
        '이력명부' : EmployeeMajorHistory,
        '구성원표' : DepartmentComposition
    }
    
    def get_data(self, data, date):
        __callable = self.__dict.get(data)
        df = __callable(date).get_df()
        return df



date = datetime(2022,4,27)

# df = DepartmentComposition(date).get_df()
# print(df)

df = DataProvider().get_data('구성원표', date)
df.to_csv('구성원표.csv', encoding='cp949')