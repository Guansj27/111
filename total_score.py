import pandas as pd
import joblib
from sklearn.preprocessing import MinMaxScaler

'''weights = {'promotion_score':0.50052929,
           'product_score':0.06524916,
           'REPLACEMENT_MARKET':0.04697414,
           'Adjust_Score':0.38724741}'''

class TotalScore:
    def __init__(self, product_score, promotion_score, carparc_score, history_score):
        self.product_score = product_score
        self.promotion_score = promotion_score
        self.carparc_score = carparc_score
        self.history_score = history_score
        self.weights = {'promotion_score':0.25,
                           'product_score':0.25,
                           'carparc_score':0.25,
                           'history_score':0.25}

    def _merge_scores(self):
        total = pd.merge(self.history_score, self.promotion_score, on=['rtlSpId','materialCode'], how='outer')
        total = total.merge(self.carparc_score, on=['rtlSpId','materialCode'], how='outer')
        product = pd.merge(self.product_score, total['rtlSpId'].drop_duplicates(), how='cross')
        total = total.merge(product, on=['rtlSpId','materialCode'], how='outer')
        total['promotion_score'] = total['startDate'].isnull().apply(lambda x: 0 if x else 1)
        total.drop(columns=['startDate','endDate'], inplace=True)
        total.fillna(0, inplace=True)
        total.drop_duplicates(inplace=True)
        total.columns = ['rtlSpId', 'materialCode',   'history_score',   'carparc_score',     'product_score',  'promotion_score']
        return total

    def _score_standard_scaler(self, df):
        name_mapping = {'promotion_score': 'promotion_score', 'product_score': 'product_score', 'REPLACEMENT_MARKET': 'carparc_score',
                        'Adjust_Score': 'history_score'}
        for key in name_mapping.keys():
            scaler = joblib.load('.\Model\\' + key + '.pkl')
            df[name_mapping[key]] = scaler.transform(df[name_mapping[key]].values.reshape(-1, 1))
        return df

    def _weighted_score(self, df):
        df['score'] = df.apply(lambda row: sum(row[col] * self.weights[col] for col in self.weights.keys()),
                                                   axis=1)
        return df

    def _ouput_formatting(self, df):
        df['score'] = df['score'].round(6)
        return df

    def process(self):
        total_score = self._merge_scores()
        total_score_scaler = self._score_standard_scaler(total_score)
        total_score_scaler = self._weighted_score(total_score_scaler)
        total_score_scaler = self._ouput_formatting(total_score_scaler)
        # total_score_scaler[['rtlSpId', 'materialCode', 'score']]
        return total_score_scaler[['rtlSpId', 'materialCode', 'score']]

'''
PRODUCT_PATH = '..\Result/product_score_2023.csv'
PROMOTION_PATH = '..\Result/promotion_score_1009.csv'
CARPARC_PATH = '..\Result/carparc_score_2023.csv'
HISTORY_ORDER_PATH = '..\Result/history_score_kmeans_1009.csv'

def read_result_file(PATH):
    return pd.read_csv(PATH, dtype={'STORE_CODE':str, 'SAP_PROD_CODE':str})
    
product = read_result_file(PRODUCT_PATH)
promotion = read_result_file(PROMOTION_PATH)
carparc = read_result_file(CARPARC_PATH)
history_order = read_result_file(HISTORY_ORDER_PATH)
total = pd.merge(history_order, promotion, on=['STORE_CODE', 'SAP_PROD_CODE'], how='outer')
print(total[(total['SAP_PROD_CODE']=='523519') & (total['STORE_CODE']=='78001946')])
total = total.merge(carparc, on=['STORE_CODE', 'SAP_PROD_CODE'], how='outer')
print(total[(total['SAP_PROD_CODE']=='523519') & (total['STORE_CODE']=='78001946')])
product = pd.merge(product, total['STORE_CODE'].drop_duplicates(), how='cross')
total = total.merge(product, on=['STORE_CODE', 'SAP_PROD_CODE'], how='outer')
print(total[(total['SAP_PROD_CODE']=='523519') & (total['STORE_CODE']=='78001946')])
print(total.head())
total['promotion_score'] = total['有效期开始时间'].isnull().apply(lambda x: 0 if x else 1)
total.drop(columns=['有效期开始时间','有效期截止时间'], inplace=True)

#scaler = MinMaxScaler()
#total['REPLACEMENT_MARKET'] = scaler.fit_transform(total['REPLACEMENT_MARKET'].values.reshape(-1, 1))
total.fillna(0,inplace=True)
total.drop_duplicates(inplace=True)
#total['Total_Score'] = total.apply(lambda row: sum(row[col] * weights[col] for col in weights.keys()), axis=1)
print(total[(total['SAP_PROD_CODE']=='523519') & (total['STORE_CODE']=='78001946')])
total.to_csv('..\Result/total_1009.csv')'''