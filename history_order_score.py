import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans
from dateutil.relativedelta import relativedelta
from datetime import datetime

from utils import read_order_data, get_date_str, score_output
import time

class HistoryOrder:

    def __init__(self, data):
        self.data = data


    # Order Recency
    def _order_recency(self, order_date, current_date):
        six_months_ago = current_date - relativedelta(months=6)
        twelve_months_ago = current_date - relativedelta(months=12)
        if order_date >= six_months_ago:
            return 'R6'
        elif order_date >= twelve_months_ago:
            return 'R12'
        else:
            return '>R12'

    def _adjust_score(self, recency, score):
        if recency=='R6':
            res = score
        elif recency == 'R12':
            res = score*0.5
        else:
            res = score*0.25
        return res

    def _num2score(self, num_list):
        '''
        按排名计算得分
        :param num_list:
        :return:
        '''
        unique_elements = np.unique(num_list)
        ranks = np.searchsorted(unique_elements, num_list)+1
        num_unique_elements = len(unique_elements)
        return ranks/(num_unique_elements + 0.000001)

    def _minmax_score(self, num_list):
        num_list = num_list.values.reshape(-1, 1)
        scaler = MinMaxScaler()
        normalized = scaler.fit_transform(num_list)
        return normalized

    def _num2cluster(self, num_list, n_clusters):
        '''
        返回聚类的label
        :param num_list:
        :param n_clusters:
        :return:
        '''
        numbers = num_list.values.reshape(-1, 1)
        kmeans = KMeans(n_clusters=n_clusters)

        # 进行聚类
        kmeans.fit(numbers)

        # 获取每个样本的聚类标签
        labels = kmeans.labels_

        # 计算每个聚类的平均值，并按照平均值的大小对聚类进行排序
        average_values = [np.mean(numbers[labels == i]) for i in range(n_clusters)]
        sorted_clusters = np.argsort(average_values)

        # 创建一个新的标签数组，其中聚类标签已按照每个聚类的平均值的大小进行排序
        new_labels = np.empty_like(labels)
        for i, cluster in enumerate(sorted_clusters):
            new_labels[labels == cluster] = i

        return new_labels

    def _kmeans_score(self, num_list, n_clusters=20):
        new_labels = self._num2cluster(num_list, n_clusters)
        cluster_df = pd.DataFrame({'NUM': num_list.values, 'CLUSTER': new_labels})
        for cluster in range(n_clusters):
            cluster_df.loc[cluster_df['CLUSTER'] == cluster, 'SCORE'] = self._num2score(
                cluster_df.loc[cluster_df['CLUSTER'] == cluster, 'NUM'])
        cluster_df['KMEANS_SCORE'] = (cluster_df['SCORE'] + cluster_df['CLUSTER']) / n_clusters
        #cluster_df.to_csv('..\Result\cluster_df.csv')
        return cluster_df['KMEANS_SCORE'].values

    # change date format
    def _date_formatting(self, date_column):
        return pd.to_datetime(date_column, format='ISO8601')

    #group order by recency, store, sku
    def _order_summary(self, current_date):
        self.data['orderDate2'] = self._date_formatting(self.data['orderDate2'])
        current_date = datetime.strptime(current_date, '%Y/%m/%d')
        # 筛选最近2年的订单
        start_date = current_date - relativedelta(months=24)
        order = self.data[(self.data['orderDate2'] <= current_date) & (self.data['orderDate2'] >= start_date)]
        order['Recency'] = order['orderDate2'].apply(lambda x: self._order_recency(x, current_date))

        #self.data = self.data[(self.data['ORDER_DATE2'] <= current_date) & (self.data['ORDER_DATE2'] >= start_date)]
        #self.data['Recency'] = self.data['ORDER_DATE2'].apply(lambda x: self._order_recency(x, current_date))
        order_summary = order.groupby(['storeCode', 'sapProdCode', 'Recency'], as_index=False).agg(
            {'quantity': 'sum', 'orderNo': 'count'})
        return order_summary

    def _single_score(self, order_summary):
        recency_list = ['R6', 'R12', '>R12']
        fm_name_dict = {"quantity": "Q_Score", "orderNo": "F_Score"}
        for key in fm_name_dict.keys():
            for recency in recency_list:
                order_summary.loc[order_summary['Recency'] == recency, fm_name_dict[key]] = self._kmeans_score(order_summary.loc[order_summary['Recency'] == recency, key])
        return order_summary

    def ouput_formatting(self, df):
        df.columns = ['rtlSpId','materialCode',	'score']
        df['score'] = df['score'].round(6)
        return df

    def total_score(self, current_date):
        order_summary = self._order_summary(current_date)
        order_summary = self._single_score(order_summary)
        order_summary['Score'] = (order_summary["Q_Score"] + order_summary["F_Score"])/2
        order_summary['Adjust_Score'] = order_summary.apply(lambda row: self._adjust_score(row['Recency'], row['Score']), axis=1)
        #order_summary.to_csv('..\Result\order_summary_kmeans.csv')
        total_score = order_summary.groupby([ 'storeCode','sapProdCode'], as_index=False).agg({'Adjust_Score': sum}).reset_index(drop=True)
        total_score['Adjust_Score'] = total_score['Adjust_Score']/1.75
        return total_score

    def get_history_score(self, df):
        new_df = pd.DataFrame()
        date_list = df.orderDate2.unique().tolist()
        for date in date_list:
            sub_df = df[df.orderDate2 == date]
            # 计算分数的日期要在order_date上减1
            score_date = date - relativedelta(days=1)
            score_date = score_date.strftime('%Y/%m/%d')
            #score_date = time.strftime("%Y/%m/%d", time.localtime(int(score_date)))
            history_score = self.total_score(score_date)
            sub_df = sub_df.merge(history_score, on=['storeCode', 'sapProdCode'], how='left')
            new_df = pd.concat([sub_df, new_df])
        return new_df

    def store_sku_sales_yearly(self, date):
        date = datetime.strptime(date, '%Y/%m/%d')
        self.data['orderDate2'] = self._date_formatting(self.data['orderDate2'])
        start_date = datetime(date.year, 1, 1)
        yearly_order = self.data[(self.data['orderDate2']<=date) & (self.data['orderDate2'] >= start_date)]
        yearly_sku_summary = yearly_order.groupby(['storeCode', 'sapProdCode'], as_index=False).agg({'quantity': 'sum'})
        return yearly_sku_summary

    def store_size_sales_yearly(self, date, size_mapping_table):
        yearly_sku_summary = self.store_sku_sales_yearly(date)
        yearly_sku_summary = yearly_sku_summary.merge(size_mapping_table, left_on = 'sapProdCode', right_on = 'SAP产品物料编号', how='left')
        yearly_size_summary = yearly_sku_summary.groupby(['storeCode', '轮胎尺寸'], as_index=False).agg({'QUANTITY': 'sum'})
        return yearly_size_summary





if __name__ == "__main__":
    ### 用s3文件替换###
    ORDER_DIR = '.\Data\order'
    order_data = read_order_data(ORDER_DIR)


    # 获取今天的日期：str
    current_date = get_date_str()

    history_order = HistoryOrder(order_data)
    history_score = history_order.total_score(current_date)
    history_score = history_order.ouput_formatting(history_score)
    # 全部结果
    OUTPUT_DIR = '.\Result\\' +  get_date_str('%Y%m%d')
    OUT_FILENAME_TOTAL = 'ai-historical-behavior-total.csv'
    OUT_PATH_TOTAL = os.path.join(OUTPUT_DIR, OUT_FILENAME_TOTAL)
    history_score.to_csv(OUT_PATH_TOTAL, index=False)


    history_score_selected = score_output(history_score, 'score', 100)

    OUT_FILENAME = 'ai-historical-behavior.csv'
    OUT_PATH = os.path.join(OUTPUT_DIR, OUT_FILENAME)
    history_score_selected.to_csv(OUT_PATH, index=False)




'''    FILE_PATH = '..\Data\order_data.csv'
    data = pd.read_csv(FILE_PATH)

    PRODUCT_PATH = '..\Data\sfe_product_info_202310120324.csv'
    product_info = pd.read_csv(PRODUCT_PATH)

    # filter cancel and reject
    df = data[data.CANCEL_DATE.isnull() & data.REJECT_DATE.isnull()]
    # filter quantity = 0
    df = df[df.QUANTITY > 0]

    history_order = HistoryOrder(df)
    history_score = history_order.total_score('2023/10/09')
    history_score.to_csv('..\Result\history_score_kmeans_1009.csv', index=False)
    new_rp = score_output(history_score, 'Adjust_Score', 100)
    new_rp['Adjust_Score'] = new_rp['Adjust_Score'].round(6)
    new_rp.to_csv('..\Result/history_score_kmeans_selected_1009.csv', index=False)'''
    #yearly_order_summary = history_order.store_size_sales_yearly('2023/10/09', product_info[['SAP产品物料编号', '轮胎尺寸']])
    #print(yearly_order_summary.head())


