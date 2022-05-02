from concurrent.futures import process
import sys
parentPath='c:/Users/user/PycharmProjects/personnelInfo' # parent 경로
sys.path.append(parentPath) # 경로 추가
import pandas as pd
from datetime import datetime
from config import config
from util import util, loadFile

class UniqueInfoDf:

    def __init__(self):
        self.df = self.get_df()

    def get_df(self):
        df = loadFile.LoadFactory().get_loader(loader='csvToPandas', fileName='0. 직원고유정보.csv', enconding='utf-8').load()
        df = self.process(df)
        return df
    
    def process(self, df):
        p1 = ('rename_cols', ['id_', 'name', 'birth', 'sex', 'veryFirstPostionLevel', 'recruitmentDate', 'resignationDate'])
        p2 = ('apply_datetime', ['birth', 'recruitmentDate', 'resignationDate'])
        p3 = ('apply_string', ['id_'])
        p4 = ('filter', ['id_', 'name', 'birth', 'sex', 'recruitmentDate', 'resignationDate'])
        process_lst = [p1, p2, p3, p4]
        pc = util.FBuilder().get_data_processing(process_lst)
        df = pc.process(df)
        return df

class ContactInfoDf:

    def __init__(self):
        self.df = self.get_df()

    def get_df(self):
        df = loadFile.LoadFactory().get_loader(loader='csvToPandas', fileName='1. 직원연락정보.csv', enconding='utf-8').load()
        df = self.process(df)
        return df

    def process(self, df):
        p1 = ('rename_cols', ['id_', 'name', 'address', 'mailAddress', 'eMail', 'innerTel', 'mobileTel', 'workFloor'])
        p2 = ('apply_string', ['id_'])
        p3 = ('filter', ['id_', 'address', 'mailAddress', 'eMail', 'innerTel', 'mobileTel', 'workFloor'])
        process_lst = [p1, p2, p3]
        pc = util.FBuilder().get_data_processing(process_lst)
        df = pc.process(df)
        return df

class AppointingInfoDf:

    def __init__(self, date):
        self.date = date
        self.df = self.get_df()

    def get_df(self):
        df = loadFile.LoadFactory().get_loader(loader='csvToPandas', fileName='4. 직원발령정보_22.4.26.csv', enconding='utf-8').load()       
        df = self.process1(df)
        df = df.loc[~df.loc[:, '사번'].isna()]
        df = self.process2(df)
        df = df.where(pd.notnull(df), None)
        df = df.loc[df.loc[:, 'appoDate'] <= self.date]
        return df

    def process1(self, df):
        p1 = ('filter', ['사원', '사번', '발령일', '발령명', '소속부서', '직위', '직급', '직책', '비고'])
        p2 = ('sort', ['사번', '발령일'])
        process_lst = [p1, p2]
        pc = util.FBuilder().get_data_processing(process_lst)
        df = pc.process(df)
        return df

    def process2(self, df):
        p1 = ('rename_cols', ['name', 'id_', 'appoDate', 'appoName', 'appoDepartment', 'appoPosition', 'appoPositionLevel', 'appoLeader', 'description'])
        p2 = ('apply_datetime', ['appoDate'])
        p3 = ('apply_string', ['id_'])
        process_lst = [p1, p2, p3]
        pc = util.FBuilder().get_data_processing(process_lst)
        df = pc.process(df)
        return df

class Temp_employeeDf:

    def __init__(self, date):
        self.date = date
        self.df = self.get_df()

    def get_df(self):
        df = loadFile.LoadFactory().get_loader(loader='csvToPandas', fileName='8. 일반사무직직원정보.csv', enconding='cp949').load()
        df = self.process(df)
        con1 = df.loc[:,'toPermanentDate'] >= self.date
        con2 = df.loc[:,'toPermanentDate'].isna()
        df = df.loc[con1|con2]
        df = df.loc[:, ['id_', 'name']]
        df = df.assign(contact_state='일반사무직')
        return df.loc[:, ['id_', 'contact_state']]

    def process(self, df):
        p1 = ('rename_cols', ['id_', 'name', 'toPermanentDate'])
        p2 = ('apply_datetime', ['toPermanentDate'])
        p3 = ('apply_string', ['id_'])
        process_lst = [p1, p2, p3]
        pc = util.FBuilder().get_data_processing(process_lst)
        df = pc.process(df)
        return df

    def get_lst(self):
        return self.df.loc[:,'id_'].to_list()

class Expert_employeeDf:

    def __init__(self, date):
        self.date = date
        self.df = self.get_df()

    def get_df(self):
        df = loadFile.LoadFactory().get_loader(loader='csvToPandas', fileName='9. 전문사무직직원정보.csv', enconding='utf-8').load()
        df = self.process(df)
        df = df.assign(contact_state='전문사무직')
        return df.loc[:, ['id_', 'contact_state']]

    def process(self, df):
        p1 = ('rename_cols', ['id_', 'name'])
        p2 = ('apply_string', ['id_'])
        process_lst = [p1, p2]
        pc = util.FBuilder().get_data_processing(process_lst)
        df = pc.process(df)
        return df

    def get_lst(self):
        return self.df.loc[:,'id_'].to_list()

class Sended_employeeDf:

    def __init__(self, date):
        self.date = date
        self.df = self.get_df()

    def get_df(self):
        df = loadFile.LoadFactory().get_loader(loader='csvToPandas', fileName='10. 파견직직원정보.csv', enconding='utf-8').load()
        df = self.process(df)
        df = df.assign(contact_state='파견직')
        return df.loc[:, ['id_', 'contact_state']]

    def process(self, df):
        p1 = ('rename_cols', ['id_', 'name'])
        p2 = ('apply_string', ['id_'])
        process_lst = [p1, p2]
        pc = util.FBuilder().get_data_processing(process_lst)
        df = pc.process(df)
        return df

    def get_lst(self):
        return self.df.loc[:,'id_'].to_list()

date = datetime(2021,12,31)
# df = Sended_employeeDf(date).df
# # print(df.to_dict(orient='records')[0].get(('his',)))
# # print(df.loc[df.loc[:, 'order'].isna()])
# print(df)

print(Sended_employeeDf(date).get_df())




