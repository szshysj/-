from os import getcwd
from os.path import join
from datetime import datetime
from re import match

import pandas as pd


def change_data(data):
    data = str(data).split()[0]
    data = data.replace('/', '-')
    data = data.split('-')

    try:
        data = data[0] + data[1]
    except IndexError:  # 对标题列进行特殊处理
        return '000000'

    return data


def consume_report_handler():
    df = pd.read_excel(cost_report, usecols=[1, 4])
    df = df.rename(columns={f'{date} Commission Basic Amount': '消耗金额', 'member_id': '客户id'})
    df = df.loc[:, ['客户id', '消耗金额']]

    if len(df[df['消耗金额'].notna()]) == 0:
        print('消耗表信息可能有误')
        exit()

    return df


def sale_form_handler():
    # Unnamed: 2 战队 5 姓名 6 客户id 9 订单属性 10 订单金额 13 到账时间 19 提成金额
    df = pd.read_excel(stats_report, header=1, usecols=[2, 5, 6, 8, 9, 10, 13, 19])

    # 改列名
    df = df.rename(columns={'Unnamed: 2': '战队',
                            'Unnamed: 5': '姓名',
                            'Unnamed: 6': '客户id',
                            'Unnamed: 8': '产品',
                            'Unnamed: 9': '订单属性',
                            'Unnamed: 10': '订单金额',
                            'Unnamed: 13': '到账时间',
                            'Unnamed: 19': '提成金额'})

    # 处理数据
    df['客户id'] = df['客户id'].apply(lambda x: str(x).replace('\n', ''))
    df['到账时间'] = df['到账时间'].fillna('0000-00-00 00:00:00')
    df['到账时间'] = df['到账时间'].apply(change_data)

    # 过滤
    df = df[(df['战队'] == '新签团队') | (df['战队'] == '续签服务团队')]
    df = df[df['订单属性'] == '新签']
    df = df[df['到账时间'] == date]
    df = df[df['提成金额'].isna()]
    df = df[df['产品'].isin(['GGS-Basic-1年', 'GGS-Basic-2年', 'GGS-Standard-1年', 'GGS-Standard-2年',
                           'GGS-Premium-1年', 'GGS-Premium-2年'])]

    return df.loc[:, ['姓名', '客户id', '订单金额']]


def join_select(status_report, cost_report_):
    df = pd.merge(status_report, cost_report_, on='客户id')
    df['牌级'] = df['订单金额'] + df['消耗金额']

    return df.loc[:, ['姓名', '客户id', '订单金额', '消耗金额', '牌级']]


def calc_func(df_join):
    name_dict = {}  # 存储首单的字典

    percent_1500 = 0.06  # 第1阶段 0-1500
    percent_3000 = 0.09  # 第2阶段 1500-3000
    percent_6000 = 0.12  # 第3阶段 3000-6000
    percent_9000 = 0.16  # 第4阶段 6000-9000
    percent_nb = 0.2  # 最终阶段 9000- 无上限, 牛逼

    special_money = None  # 1500-消耗额

    result_list = []  # 存放计算结果

    # 遍历每个销售
    for index, group in df_join.groupby('姓名'):

        # 遍历当前销售的所有销售业绩
        for people in group.values:
            #    姓名          客户id          订单金额   消耗金额    牌级
            # ['吴优'    'cz1256808397knjj'     1259        5      1264.0]

            # 首单进入这里, 不用往下看
            if people[0] not in name_dict:

                # 只要出单金额加消耗的大于1500元，都要用第1阶段的1500 - 消耗
                # 有以下几种情况
                # 1399 + 消耗 > 1500元
                # 2399
                # 5399
                if people[-1] > 1500:

                    # 每个阶段均复用下面这行代码
                    special_money = 1500 - people[-2]

                    # 专门处理特殊情况 1399的单子加消耗大于1500, 大于1399的单子不用看下面的代码
                    if 0 < people[2] <= 1500:
                        money_1 = special_money * percent_1500
                        money_1 += (people[-1] - 1500) * percent_3000
                        name_dict[people[0]] = round(money_1, 2)
                        result_list.append([people[0], people[1], people[2], people[-2], round(money_1, 2)])
                        continue

                # =================================我是分割线=============================================

                # 1399 + 消耗 < 1500 元
                if 0 < people[2] <= 1500:
                    money_1 = people[2] * percent_1500
                    name_dict[people[0]] = round(money_1, 2)

                # 1259*2  1399*2  2399
                elif 1500 < people[2] <= 3000:
                    money_1 = special_money * percent_1500
                    money_2 = (people[2] - special_money) * percent_3000
                    name_dict[people[0]] = round(money_1 + money_2, 2)

                # 5399
                elif 3000 < people[2] <= 6000:
                    money_1 = special_money * percent_1500
                    money_2 = 1500 * percent_3000
                    money_3 = (people[2] - special_money - 1500) * percent_6000
                    name_dict[people[0]] = round(money_1 + money_2 + money_3, 2)

                # 很尴尬的阶段, 可能永远都走不到这里, 但还是得按步骤写
                elif 6000 < people[2] <= 9000:
                    money_1 = special_money * percent_1500
                    money_2 = 1500 * percent_3000
                    money_3 = 3000 * percent_6000
                    money_4 = (people[2] - special_money - 1500 - 3000) * percent_9000
                    name_dict[people[0]] = round(money_1 + money_2 + money_3 + money_4, 2)

                # 5399*2
                elif 9000 < people[2]:
                    money_1 = special_money * percent_1500
                    money_2 = 1500 * percent_3000
                    money_3 = 3000 * percent_6000
                    money_4 = 3000 * percent_9000
                    money_5 = (people[2] - special_money - 1500 - 3000 - 3000) * percent_nb
                    name_dict[people[0]] = round(money_1 + money_2 + money_3 + money_4 + money_5, 2)

                result_list.append([people[0], people[1], people[2], people[-2], name_dict[people[0]]])
                continue

            # ======================================我是分割线===================================================

            # 首单之后正常计算提成
            if 0 < people[2] <= 1500:
                result_list.append([people[0], people[1], people[2], '', people[2] * percent_1500])
                continue

            elif 1500 < people[2] <= 3000:
                money_1 = 1500 * percent_1500
                money_2 = ((people[2] - 1500) * percent_3000) + money_1
                result_list.append([people[0], people[1], people[2], '', money_2])
                continue

            elif 3000 < people[2] <= 6000:
                money_1 = 1500 * percent_1500
                money_2 = 1500 * percent_3000
                money_3 = ((people[2] - 1500 - 1500) * percent_6000) + money_1 + money_2
                result_list.append([people[0], people[1], people[2], '', money_3])
                continue

            elif 6000 < people[2] <= 9000:
                money_1 = 1500 * percent_1500
                money_2 = 1500 * percent_3000
                money_3 = 3000 * percent_6000
                money_4 = ((people[2] - 1500 - 1500 - 3000) * percent_9000) + money_1 + money_2 + money_3
                result_list.append([people[0], people[1], people[2], '', money_4])
                continue

            elif 9000 < people[2]:
                money_1 = 1500 * percent_1500
                money_2 = 1500 * percent_3000
                money_3 = 3000 * percent_6000
                money_4 = 3000 * percent_9000
                money_5 = ((people[2] - 1500 - 1500 - 3000 - 3000) * percent_nb) + money_1 + money_2 + money_3 + money_4
                result_list.append([people[0], people[1], people[2], '', money_5])
                continue

    return result_list


def write_data(result_):
    df = pd.DataFrame(result_, columns=['姓名', '客户id', '订单金额', '消耗金额', '提成金额'])

    pd.DataFrame(df).to_excel(f'{date}报表.xlsx', sheet_name='Sheet1', index=False, header=True)


def main():
    # 整理数据
    df_status = sale_form_handler()
    df_cost = consume_report_handler()

    # 关联数据
    df_join = join_select(df_status, df_cost)

    # 计算数据
    result = calc_func(df_join)

    # 数据写入到文件
    write_data(result)


if __name__ == '__main__':

    while 1:
        date = input('准备工作---->\n\n统计表请改名 "GGS付款统计表"\n消耗表请改名为 "消耗表"\n\n请输入要计算的月份, 例如: 201909, 输入0则退出\n')

        if int(date) == 0:
            exit()

        if len(date) > 6:
            print('{长度}有误! 请参考正确格式, 如有疑问, 请联系我!\n')
            continue

        # 验证格式
        try:
            match('\\d{4}\\d{2}', date).group(0)
        except AttributeError:
            print('{格式}有误! 请参考正确格式, 如有疑问, 请联系我!\n')
            continue

        if date >= datetime.strftime(datetime.now(), '%Y%m'):
            print('输入的日期不能{大于等于}当月!\n')
            continue

        break

    # 文件路径
    stats_report = join(getcwd(), 'GGS付款统计表.xlsx')
    cost_report = join(getcwd(), '消耗表.xlsx')

    # 主入口
    main()
