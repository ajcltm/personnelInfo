import sys
parentPath='c:/Users/user/PycharmProjects/personnelInfo' # parent 경로
sys.path.append(parentPath) # 경로 추가
import pandas as pd
from datetime import datetime
from config import config
from util import Preprocessing

class 