import sys
parentPath='c:/Users/ajcltm/PycharmProjects/personnelInfo' # parent 경로
sys.path.append(parentPath) # 경로 추가
from config import config
import pandas as pd

class Preprocessing:

    def __init__(self, df):
        self.df_ = df.copy()
        self.commands = []

    def register_commands(self, fn, *arg):
        function_option = {
            'rename_cols': self.rename_cols,
            'apply_datetime': self.apply_datetime,
            'apply_string' : self.apply_string
        }
        function_ = function_option.get(fn)
        command = (function_, arg)
        self.commands.append(command)

    def execute(self):
        for item in self.commands:
            function_ = item[0]
            var = item[1]
            function_(var)

    def rename_cols(self, cols):
        self.df_.columns = cols[0]

    def apply_datetime(self, cols):
        for col in cols[0]:
            self.df_[col] = pd.to_datetime(self.df_[col], format='%Y-%m-%d')
        
    def apply_string(self, cols):
        for col in cols[0]:
            self.df_[col] = self.df_[col].apply(lambda x: str(int(x)))
    

def preprocess_unique_information_df(df):
    re_cols = ['id_', 'name', 'birth', 'sex', 'veryFirstPostionLevel', 'recruitmentDate', 'resignationDate']
    date_cols = ['birth', 'recruitmentDate', 'resignationDate']
    string_cols = ['id_']
    pr = Preprocessing(df)
    pr.register_commands('rename_cols', re_cols)
    pr.register_commands('apply_datetime', date_cols)
    pr.register_commands('apply_string', string_cols)
    pr.execute()
    return pr.df_

def preprocess_contact_information_df(df):
    re_cols = ['id_', 'name', 'address', 'mailAddress', 'eMail', 'innerTel', 'mobileTel', 'workFloor']
    string_cols = ['id_']
    pr = Preprocessing(df)
    pr.register_commands('rename_cols', re_cols)
    pr.register_commands('apply_string', string_cols)
    pr.execute()
    return pr.df_

def preprocess_appointing_info_df(df):
    re_cols = ['name', 'id_', 'appoDate', 'appoName', 'appoDepartment', 'appoPosition', 'appoPositionLevel', 'appoLeader', 'description']
    date_cols = ['appoDate']
    string_cols = ['id_']
    pr = Preprocessing(df)
    pr.register_commands('rename_cols', re_cols)
    pr.register_commands('apply_datetime', date_cols)
    pr.register_commands('apply_string', string_cols)
    pr.execute()
    return pr.df_

def preprocess_temp_employeeDf(df):
    re_cols = ['id_', 'name', 'toPermanentDate']
    date_cols = ['toPermanentDate']
    string_cols = ['id_']
    pr = Preprocessing(df)
    pr.register_commands('rename_cols', re_cols)
    pr.register_commands('apply_datetime', date_cols)
    pr.register_commands('apply_string', string_cols)
    pr.execute()
    return pr.df_

def preprocess_sended_employeeDf(df):
    re_cols = ['id_', 'name']
    string_cols = ['id_']
    pr = Preprocessing(df)
    pr.register_commands('rename_cols', re_cols)
    pr.register_commands('apply_string', string_cols)
    pr.execute()
    return pr.df_

def get_history_format(dic):
    appoDate_ = dealNone(dic.get('appoDate').strftime(format='%Y-%m-%d'))
    appoDepartment_ = dealNone(dic.get('appoDepartment'))
    appoPositionLevel_ = dealNone(dic.get('appoPositionLevel'))
    appoLeader_ = dealNone(dic.get('appoLeader'))
    description_ = dic.get('description')
    description_ = dealNone(description_, f'({description_})')

    format = f'{appoDate_} {appoDepartment_} {appoLeader_} {appoPositionLevel_}  {description_}'
    return format

def dealNone(str, returnValue=None):
    if not str:
        return ''
    if returnValue:
        return returnValue
    return str

class Order:

    def __init__(self, df):
        self.df = df
        self.commands = []
    
    def register(self, fn, *args):
        function_option = {
            'order_level': self.order_level,
            'order_department': self.order_department,
        }
        function_ = function_option.get(fn)
        self.commands.append((function_, args))

    def execute(self):
        for item in self.commands:
            function_ = item[0]
            var = item[1]
            function_(var)

    def order_level(self, agrs):
        dic = {'사무총장' : 1, '위원장':2, '운영국장':3, '본부장':4,
                '1급':5, '2급':6, '3급':7, '4급':8, '5급':9, '6급':7,
                '전문사무직 가급': 8, '전문사무직 나급':9, '전문사무직 다급': 10,
                '파견5급': 11, '파견6급': 12
        }
        order_df = pd.DataFrame(data=list(dic.items()), columns=['positionLevel', 'level_order'])
        self.df=pd.merge(self.df, order_df, how='left',left_on=agrs[0], right_on='positionLevel')

    def order_department(self, agrs):
        date = agrs[0]
        on_ = agrs[1]
        filePath = config.get_file_path('10. 직제정보.csv')
        order_df = pd.read_csv(filePath, encoding='utf-8')
        order_df['start'] = pd.to_datetime(order_df['start'])
        order_df['end'] = pd.to_datetime(order_df['end'])
        con1 = order_df.loc[:, 'start'] <= date
        con2 = order_df.loc[:, 'end'] >= date
        order_df = order_df.loc[con1&con2]
        self.df=pd.merge(self.df, order_df, how='left',left_on=on_, right_on='department')

    def execute(self):
        for item in self.commands:
            function_ = item[0]
            var = item[1]
            function_(var)