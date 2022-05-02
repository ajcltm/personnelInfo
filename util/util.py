import sys
parentPath='c:/Users/user/PycharmProjects/personnelInfo' # parent 경로
sys.path.append(parentPath) # 경로 추가
from config import config
from typing_extensions import Protocol
import pandas as pd

class DataProcessing(Protocol):
    def process(self, df):
        ...


class Rename_cols:

    def __init__(self, cols):
        self.cols = cols

    def process(self, df):
        df.columns = self.cols
        return df


class Apply_datetime:

    def __init__(self, cols):
        self.cols = cols
    
    def process(self, df) :
        for col in self.cols:
            df[col] = pd.to_datetime(df[col], format='%Y-%m-%d')
        return df


class Apply_string:

    def __init__(self, cols):
        self.cols = cols

    def process(self, df):
        for col in self.cols:
            df[col] = df[col].apply(lambda x: str(int(x)))
        return df

class Filter:
    def __init__(self, cols):
        self.cols = cols

    def process(self, df):
        return df[self.cols]

class Sort:
    def __init__(self, cols):
        self.cols = cols

    def process(self, df):
        return df.sort_values(by=self.cols)

class Composit:

    def __init__(self, processLst):
        self.processLst = processLst

    def process(self, df):
        _df = df
        for i in self.processLst:
            _df = i[0](i[1]).process(_df)
        return _df

class FBuilder:
    _dict = {
        'rename_cols' : Rename_cols,
        'apply_datetime' : Apply_datetime,
        'apply_string' : Apply_string,
        'filter' : Filter,
        'sort' : Sort
    }

    def get_data_processing(self, processLst):
        lst = [(self._dict.get(i[0]), i[1]) for i in processLst]
        return Composit(lst)

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
        filePath = config.get_file_path('11. 직제정보.csv')
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