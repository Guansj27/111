# --用于处理订单数据的Python脚本，主要包括从Amazon S3读取数据，处理数据，然后将结果写回S3的功能
import pandas as pd
import datetime
import os
import boto3
from io import StringIO


def read_from_s3(directory_name, dtype=None, cols=None):
    s3 = boto3.client('s3',
                      region_name='cn-northwest-1')

    # 获取存储桶中的所有文件
    objects = s3.list_objects(Bucket='gy-cn-sfe-s3-prod', Prefix=directory_name)['Contents']
    all_data = pd.DataFrame()

    # 遍历存储桶中的每一个文件
    for obj in objects:
        # 跳过目录名
        if obj['Key'] == directory_name:
            continue
        # 获取文件对象
        file_obj = s3.get_object(Bucket='gy-cn-sfe-s3-prod', Key=obj['Key'])
        data = StringIO(file_obj['Body'].read().decode('utf-8'))
        df = pd.read_csv(data, dtype=dtype, usecols=cols)
        all_data = pd.concat([all_data, df])
    return all_data


def write_s3_files(df, directory_name, file_name):
    s3 = boto3.client('s3',
                      region_name='cn-northwest-1')

    # 将DataFrame转换为CSV
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # 将CSV写入S3
    key = 'ai/' + directory_name + '/' + file_name
    s3.put_object(Bucket='gy-cn-sfe-s3-prod', Key=key, Body=csv_buffer.getvalue())


def filter_cooper(df, cp_list):
    df = df[~df.materialCode.isin(cp_list)]
    return df


def concat_files(FILE_DIR):
    files = os.listdir(FILE_DIR)
    dfs = []

    for e in files:
        df = pd.read_csv(os.path.join(FILE_DIR, e), dtype={'storeCode': str, 'sapProdCode': str})
        dfs.append(df)
    return pd.concat(dfs)


def order_data_selection(data):
    # filter cancel and reject
    df = data[data.cancelDate.isnull() & data.rejectDate.isnull()]
    # filter quantity = 0
    df = df[df.quantity > 0]
    return df


def read_order_data(FILE_PATH):
    order_data = concat_files(FILE_PATH)
    order_data = order_data_selection(order_data)
    return order_data


# 获取今天的日期：str
def get_date_str(date_format='%Y/%m/%d'):
    now_time = datetime.datetime.now().strftime(date_format)
    return now_time


# 筛选前100个
def score_output(score_df, score_col, max_row):
    return score_df.groupby('rtlSpId').apply(lambda x: x.nlargest(max_row, score_col, keep='all')).reset_index(
        drop=True)


def replace_values(df):
    df.loc[df['市'] == '县', '市'] = df['省']
    df.loc[df['市'] == '市辖区', '市'] = df['省']
    df.loc[df['市'] == '省直辖县级行政区划', '市'] = df['省'] + '直辖'
    df.loc[df['市'] == '自治区直辖县级行政区划', '市'] = '新疆维吾尔自治区直辖'
    return df[['客户编号', '市']]
