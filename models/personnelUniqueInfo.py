import sys
parentPath='c:/Users/user/PycharmProjects/personnelInfo' # parent 경로
sys.path.append(parentPath) # 경로 추가
from dataclasses import dataclass, field
from typing import List
import pandas as pd
from datetime import datetime
from config import config


filePath = config.get_file_path('0. 직원고유정보.csv')

@dataclass
class PersonnelUniqueInfo:
    id_ : str
    name_ : str
    birth_ : str
    sex_ : str
    veryFirstPostionLevel: str
    recruitmentDate_ : datetime
    resignationDate_ : datetime

@dataclass
class PersonnelUniqueInfoDB:
    data : List = field(default=List)

    def __post_init__(self):
        cols = ['id_', 'name_', 'birth_', 'sex_', 'veryFirstPostionLevel', 'recruitmentDate_', 'resignationDate_']
        temp_df = pd.read_csv(filePath, encoding='utf-8')
        temp_df.columns = cols
        temp_dic = temp_df.to_dict(orient='records')
        self.data = [PersonnelUniqueInfo(**dic) for dic in temp_dic]

print(PersonnelUniqueInfoDB())