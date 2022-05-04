import sys
from numpy import uint
parentPath='c:/Users/user/PycharmProjects/personnelInfo' # parent 경로
sys.path.append(parentPath) # 경로 추가
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from config import config
from util import util
from factory import simpleModelFactory


class AppointingInfoAddedDf:

    def __init__(self, date):
        self.date = date
        self.appoDf = self.get_appoDf()
        self.df = self.get_df()

    def get_df(self):
        df1 = self.get_departMentDate_added_df()
        df2 = self.get_levelDate_added_df()
        df = pd.merge(df1, df2, how='left', on=['id_', 'appoDate'])
        df = pd.merge(self.appoDf, df, how='left', on=['id_', 'appoDate'])
        return df

    def get_appoDf(self):
        appoDf = simpleModelFactory.AppointingInfoDf(self.date).df
        appoDf = appoDf.sort_values(by=['id_', 'appoDate'], ascending=True)
        return appoDf

    def get_departMentDate_added_df(self):
        appodic = self.appoDf.to_dict(orient='records')

        lst = []
        old_id, old_department, old_department_date = None, None, None
        
        for dic in appodic:
            dic = {item[0]:item[1] for item in dic.items()}
            id_ = dic.get('id_')
            department_ = dic.get('appoDepartment')
            department_date = dic.get('appoDate')
            if not old_id == id_:
                old_id, old_department, old_department_date = id_, department_, department_date
                dic['appoDepartment_date'] = department_date
            elif not old_department == department_:
                old_department, old_department_date = department_, department_date
                dic['appoDepartment_date'] = department_date
            dic['appoDepartment_date'] = old_department_date
            lst.append(dic)
        return pd.DataFrame(data = lst).loc[:, ['id_', 'appoDate', 'appoDepartment_date']]

    def get_levelDate_added_df(self):
        appodic = self.appoDf.to_dict(orient='records')

        lst = []
        old_id, old_level, old_level_date = None, None, None
        
        for dic in appodic:
            dic = {item[0]:item[1] for item in dic.items()}
            id_ = dic.get('id_')
            level_ = dic.get('appoPositionLevel')
            level_date = dic.get('appoDate')
            if not old_id == id_:
                old_id, old_level, old_level_date = id_, level_, level_date
                dic['appoPositionLevel_date'] = level_date
            elif not old_level == level_:
                old_level, old_level_date = level_, level_date
                dic['appoPositionLevel_date'] = level_date
            dic['appoPositionLevel_date'] = old_level_date
            lst.append(dic)
        return pd.DataFrame(data = lst).loc[:, ['id_', 'appoDate', 'appoPositionLevel_date']]

class AppointingHistoryDf:
    
    def __init__(self, date):
        self.date = date
        self.df = self.get_df()

    def get_df(self):

        df = simpleModelFactory.AppointingInfoDf(self.date).df
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

        # df = pd.DataFrame(data = list(his_dic.items()), columns=[['id_', 'his']])
        df = pd.DataFrame(his_dic.values(), index=his_dic.keys()).reset_index()
        df.columns = ['id_', 'his']
        return df

class DepartmentPositionLevelDf:
    
    def __init__(self, date):
        self.date = date
        self.df = self.get_df()

    def get_df(self):
        self.df = self.get_department_position_level_df()
        df = self.merge_order()
        return df

    def get_department_position_level_df(self):
        appoDf = AppointingInfoAddedDf(self.date).df
        self.appoDf = appoDf.sort_values(by=['id_', 'appoDate'], ascending=False)
        appodic = self.appoDf.to_dict(orient='records')

        container_dic = {}

        for i in appodic:
            id_ = i.get('id_')
            if not id_ in container_dic:
                container_dic[id_] = {}
                container_dic[id_]['appoDepartment'] = i.get('appoDepartment')
                container_dic[id_]['appoDepartment_date'] = i.get('appoDepartment_date')
                r = relativedelta(self.date, i.get('appoDepartment_date'))
                container_dic[id_]['appoDepart_to_today'] = r.months + (12*r.years)
                container_dic[id_]['appoPositionLevel'] = i.get('appoPositionLevel')
                container_dic[id_]['appoPositionLevel_date'] = i.get('appoPositionLevel_date')
                r = relativedelta(self.date, i.get('appoPositionLevel_date'))
                container_dic[id_]['appoPosLevel_to_today'] = r.months + (12*r.years)
                container_dic[id_]['appoLeader'] = i.get('appoLeader')
        df = pd.DataFrame(container_dic.values(), index=container_dic.keys())
        df = df.reset_index()
        df.rename(columns={'index':'id_'}, inplace=True)
        return df

    def merge_order(self):
        o = util.Order(self.df)
        o.register('order_level', 'appoPositionLevel')
        o.register('order_department', self.date, 'appoDepartment')
        o.execute()

        # dic = {'사무총장' : 1, '위원장':2, '운영국장':3, '본부장':4,
        #         '1급':5, '2급':6, '3급':7, '4급':8, '5급':9, '6급':7,
        #         '전문사무직 가급': 8, '전문사무직 나급':9, '전문사무직 다급': 10,
        #         '파견5급': 11, '파견6급': 12
        # }
        # order_df = pd.DataFrame(data=list(dic.items()), columns=['positionLevel', 'level_order'])
        # df=pd.merge(self.df, order_df, how='left',left_on='appoPositionLevel', right_on='positionLevel')
        # df = df.loc[:,['id_', 'appoDepartment', 'appoDepartment_date', 'appoPositionLevel', 'appoPositionLevel_date', 'level_order']]
        return o.df.loc[:,['id_', 'appoDepartment', 'appoDepartment_date', 'appoDepart_to_today', 'appoPositionLevel', 'appoPositionLevel_date', 'appoPosLevel_to_today', 'level_order', 'depart_order', 'appoLeader']]

class ContactState:

    def __init__(self, date):
        self.date = date
        self.df = self.get_df()

    def get_df(self):
        uDf = simpleModelFactory.UniqueInfoDf().df
        tDf = simpleModelFactory.Temp_employeeDf(self.date).df
        eDf = simpleModelFactory.Expert_employeeDf(self.date).df
        sDf = simpleModelFactory.Sended_employeeDf(self.date).df
        tesDf = pd.concat([tDf, eDf, sDf])
        df = pd.merge(uDf, tesDf, how='left', on='id_')
        df = df.loc[:, ['id_', 'contact_state']]
        self.df = df.fillna('정규직')
        df = self.merge_order()
        return df

    def merge_order(self):
        dic = {'정규직' : 1, '계약직':2, '파견직':3}
        order_df = pd.DataFrame(data=list(dic.items()), columns=['contact_state', 'contact_order'])
        df=pd.merge(self.df, order_df, how='left', on='contact_state')
        # df = df.loc[:,['id_', 'appoDepartment', 'appoDepartment_date', 'appoPositionLevel', 'appoPositionLevel_date', 'level_order']]
        return df

# date = datetime(2020,12,31)
# df = AppointingHistoryDf(date).df
# # print(df.to_dict(orient='records')[0].get(('his',)))
# # print(df.loc[df.loc[:, 'order'].isna()])
# print(df)
# df.to_csv('history.csv',encoding='cp949')