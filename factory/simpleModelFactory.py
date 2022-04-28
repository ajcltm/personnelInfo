import sys
parentPath='c:/Users/user/PycharmProjects/personnelInfo' # parent 경로
sys.path.append(parentPath) # 경로 추가
import pandas as pd
from datetime import datetime
from config import config
from util import util

class UniqueInfoDf:

    def __init__(self):
        self.df = self.get_df()

    def get_df(self):
        filePath = config.get_file_path('0. 직원고유정보.csv')
        df = pd.read_csv(filePath, encoding='utf-8')
        df = util.preprocess_unique_information_df(df)
        return df.loc[:, ['id_', 'name', 'birth', 'sex', 'recruitmentDate', 'resignationDate']]

class ContactInfoDf:

    def __init__(self):
        self.df = self.get_df()

    def get_df(self):
        filePath = config.get_file_path('1. 직원연락정보.csv')
        df = pd.read_csv(filePath, encoding='utf-8')
        df = util.preprocess_contact_information_df(df)
        return df.loc[:, ['id_', 'address', 'mailAddress', 'eMail', 'innerTel', 'mobileTel', 'workFloor']]

class AppointingInfoDf:

    def __init__(self, date):
        self.date = date
        self.df = self.get_df()

    def get_df(self):
        filePath = config.get_file_path('4. 직원발령정보_22.4.26.csv')
        df = pd.read_csv(filePath, encoding='utf-8')
        filted = ['사원', '사번', '발령일', '발령명', '소속부서', '직위', '직급', '직책', '비고']
        df = df[filted]
        df = df.sort_values(by=['사번', '발령일'])
        df = df.loc[~df.loc[:, '사번'].isna()]
        df = util.preprocess_appointing_info_df(df)
        df = df.where(pd.notnull(df), None)
        df = df.loc[df.loc[:, 'appoDate'] <= self.date]
        return df

class Temp_employeeDf:

    def __init__(self, date):
        self.date = date
        self.df = self.get_df()

    def get_df(self):
        filePath = config.get_file_path('8. 일반사무직직원정보.csv')
        df = pd.read_csv(filePath, encoding='cp949')
        df = util.preprocess_temp_employeeDf(df)
        con1 = df.loc[:,'toPermanentDate'] >= self.date
        con2 = df.loc[:,'toPermanentDate'].isna()
        df = df.loc[con1|con2]
        df = df.loc[:, ['id_', 'name']]
        df = df.assign(contact_state='일반사무직')
        return df.loc[:, ['id_', 'contact_state']]

    def get_lst(self):
        return self.df.loc[:,'id_'].to_list()

class Expert_employeeDf:

    def __init__(self, date):
        self.date = date
        self.df = self.get_df()

    def get_df(self):
        filePath = config.get_file_path('9. 전문사무직직원정보.csv')
        df = pd.read_csv(filePath, encoding='utf')
        df = util.preprocess_expert_employeeDf(df)
        df = df.assign(contact_state='전문사무직')
        return df.loc[:, ['id_', 'contact_state']]

    def get_lst(self):
        return self.df.loc[:,'id_'].to_list()

class Sended_employeeDf:

    def __init__(self, date):
        self.date = date
        self.df = self.get_df()

    def get_df(self):
        filePath = config.get_file_path('10. 파견직직원정보.csv')
        df = pd.read_csv(filePath, encoding='utf-8')
        df = util.preprocess_sended_employeeDf(df)
        df = df.assign(contact_state='파견직')
        return df.loc[:, ['id_', 'contact_state']]

    def get_lst(self):
        return self.df.loc[:,'id_'].to_list()

# date = datetime(2021,12,31)
# df = Sended_employeeDf(date).df
# # print(df.to_dict(orient='records')[0].get(('his',)))
# # print(df.loc[df.loc[:, 'order'].isna()])
# print(df)




