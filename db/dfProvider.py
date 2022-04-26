import sys
parentPath='c:/Users/user/PycharmProjects/personnelInfo' # parent 경로
sys.path.append(parentPath) # 경로 추가
import pandas as pd
from datetime import datetime
from config import config
from util import util

class UniqueInfoDf:

    def __init__(self):
        self.df = self.get_unique_info_df()

    def get_unique_info_df(self):
        filePath = config.get_file_path('0. 직원고유정보.csv')
        df = pd.read_csv(filePath, encoding='utf-8')
        df = util.preprocess_unique_information_df(df)
        return df

class ContactInfoDf:

    def __init__(self):
        self.df = self.get_contact_info_df()

    def get_contact_info_df():
        filePath = config.get_file_path('1. 직원연락정보.csv')
        df = pd.read_csv(filePath, encoding='utf-8')
        df = util.preprocess_contact_information_df(df)
        return df

class AppointingInfoDf:

    def __init__(self, date):
        self.date = date
        self.df = self.get_appointing_info_df()

    def get_appointing_info_df(self):
        filePath = config.get_file_path('4. 직원발령정보.csv')
        df = pd.read_csv(filePath, encoding='utf-8')
        filted = ['사원', '사번', '발령일', '발령명', '소속부서', '직위', '직급', '직책', '비고']
        df = df[filted]
        df = df.sort_values(by=['사번', '발령일'])
        df = df.loc[~df.loc[:, '사번'].isna()]
        df = util.preprocess_appointing_info_df(df)
        df = df.where(pd.notnull(df), None)
        df = df.loc[df.loc[:, 'appoDate_'] <= self.date]
        return df

class AppointingHistoryDf:
    
    def __init__(self, date):
        self.date = date
        self.df = self.get_appointing_history_df()

    def get_appointing_history_df(self) :

        df = AppointingInfoDf(self.date).get_appointing_info_df()
        dic = df.to_dict(orient='records')
        
        his_dic = {}
        for i in dic :
            id_ = i.get('id_')
            if not id_ in his_dic:
                his_dic[id_] = []
            # if len(his_dic[id_])>0:
            #     his_dic[id_].append('\n')
            his_dic[id_].append(util.get_history_format(i))

        for i in his_dic.items():
            his_dic[i[0]] = '\n'.join(his_dic[i[0]])

        return pd.DataFrame(data = list(his_dic.items()), columns=[['id_', 'his']])

class DepartmentPositionLevelDf:
    
    def __init__(self, date):
        self.date = date
        self.df = self.get_df()

    def get_appoDf(self):
        appoDf = AppointingInfoDf(self.date).get_appointing_info_df()
        self.appoDf = appoDf.sort_values(by=['id_', 'appoDate_'], ascending=False)
        appodic = self.appoDf.to_dict(orient='records')

        old_id, old_level, old_level_date = None, None, None
        for dic in appodic:
            id_ = dic.get('id_')
            level_ = dic.get('appoPositionLevel_')
            level_date = dic.get('appoDate_')
            if not old_id == id_:
                old_id, old_level = id_, level_
                dic['appoPositionLevel_date'] = level_date


    def get_df(self):
        self.df = self.get_department_position_level_df()
        self.df = self.get_upLevel_date()
        df = self.merge_order()
        return df

    def get_department_position_level_df(self):
        appoDf = AppointingInfoDf(self.date).get_appointing_info_df()
        self.appoDf = appoDf.sort_values(by=['id_', 'appoDate_'], ascending=False)
        appodic = self.appoDf.to_dict(orient='records')

        container_dic = {}

        for i in appodic:
            id_ = i.get('id_')
            if not id_ in container_dic:
                container_dic[id_] = {}
                container_dic[id_]['appoDepartment_'] = i.get('appoDepartment_')
                container_dic[id_]['appoPositionLevel_'] = i.get('appoPositionLevel_')
        df = pd.DataFrame(container_dic.values(), index=container_dic.keys())
        df = df.reset_index()
        df.rename(columns={'index':'id_'}, inplace=True)
        return df
    
    def get_upLevel_date(self):
        result_dic = self.df.to_dict(orient='records')
        for i in result_dic:
            id_ = i.get('id_')
            con=self.appoDf.loc[:, 'id_']==id_
            temp_df = self.appoDf.loc[con]
            temp_dic = temp_df.to_dict(orient='records')
            for w in temp_dic :
                if i.get('appoPositionLevel_') != w.get('appoPositionLevel_'):
                    i['appoPositionLevel_date_'] = w.get('')

    def merge_order(self):
        dic = {'사무총장' : 1, '위원장':2, '운영국장':3, '본부장':4,
                '1급':5, '2급':6, '3급':7, '4급':8, '5급':9, '6급':7
        }
        order_df = pd.DataFrame(data=list(dic.items()), columns=['positionLevel_', 'order'])
        df=pd.merge(self.df, order_df, how='left',left_on='appoPositionLevel_', right_on='positionLevel_')
        df = df.iloc[:,[0,1,2,4]]
        return df
                
class DbAtTime:
    def __init__(self, date):
        self.date=date
        self.dfs = []
        self.df = self.execute()
    
    def register_df(self, df):
        self.dfs.append(df)
    
    def compose_df(self):
        df = UniqueInfoDf().df.loc[:, 'quit']

    def execute(self):
        df = self.dfs[0]
        for i in self.dfs[1:]:
            df = pd.merge(df, i, how='left', on='id_')
        return df

date = datetime(2020,12,31)
df = DepartmentPositionLevelDf(date).df
print(df.loc[df.loc[:, 'order']>=1])



