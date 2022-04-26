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
    re_cols = ['id_', 'name_', 'birth_', 'sex_', 'veryFirstPostionLevel_', 'recruitmentDate_', 'resignationDate_']
    date_cols = ['birth_', 'recruitmentDate_', 'resignationDate_']
    string_cols = ['id_']
    pr = Preprocessing(df)
    pr.register_commands('rename_cols', re_cols)
    pr.register_commands('apply_datetime', date_cols)
    pr.register_commands('apply_string', string_cols)
    pr.execute()
    return pr.df_

def preprocess_contact_information_df(df):
    re_cols = ['id_', 'name_', 'address_', 'mailAddress_', 'eMail_', 'innerTel_', 'mobileTel_', 'workFloor_']
    string_cols = ['id_']
    pr = Preprocessing(df)
    pr.register_commands('rename_cols', re_cols)
    pr.register_commands('apply_string', string_cols)
    pr.execute()
    return pr.df_

def preprocess_appointing_info_df(df):
    re_cols = ['name_', 'id_', 'appoDate_', 'appoName_', 'appoDepartment_', 'appoPosition_', 'appoPositionLevel_', 'appoLeader_', 'description_']
    date_cols = ['appoDate_']
    string_cols = ['id_']
    pr = Preprocessing(df)
    pr.register_commands('rename_cols', re_cols)
    pr.register_commands('apply_datetime', date_cols)
    pr.register_commands('apply_string', string_cols)
    pr.execute()
    return pr.df_

def get_history_format(dic):
    appoDate_ = dealNone(dic.get('appoDate_').strftime(format='%Y-%m-%d'))
    appoDepartment_ = dealNone(dic.get('appoDepartment_'))
    appoPositionLevel_ = dealNone(dic.get('appoPositionLevel_'))
    appoLeader_ = dealNone(dic.get('appoLeader_'))
    description_ = dic.get('description_')
    description_ = dealNone(description_, f'({description_})')

    format = f'{appoDate_} {appoDepartment_} {appoLeader_} {appoPositionLevel_}  {description_}'
    return format

def dealNone(str, returnValue=None):
    if not str:
        return ''
    if returnValue:
        return returnValue
    return str
