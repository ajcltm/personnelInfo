from pathlib import Path

userName = 'user'
desktopPath = Path().home().joinpath('Desktop')
folderPath = desktopPath.joinpath('업무', '2020년 업무', '분류별', '인사(22년)', '1. 인사자료', '1. 인사 데이터')  # 업무\2020년 업무\분류별\인사(22년)\1. 인사자료


def get_file_path(fileName):
    return folderPath.joinpath(f'{fileName}')


if __name__ == '__main__':
    path = get_file_path('0. 직원고유정보.csv')
    print(path)
    import pandas as pd
    print(pd.read_csv(path, encoding='utf-8'))