import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import os
from datetime import datetime, timedelta
from utils import get_date_str

class PromotionScore:

    def __init__(self, data):
        self.data = data
        self.data = self.data[self.data['productCode'].apply(lambda x: str(x).isdigit() and len(str(x)) == 6)]

    def _date_formatting(self):
        self.data['dateEff'] = pd.to_datetime(self.data['dateEff'], format='ISO8601')
        self.data['dateExp'] = pd.to_datetime(self.data['dateExp'], format='ISO8601')

    def _promote_score(self, target_date, preparation_period=0):
        promotion_data = self.data[(self.data['dateEff'] <= (target_date - timedelta(days=preparation_period))) & (self.data['dateExp'] >= target_date)]
        return promotion_data[['storeCode', 'productCode', 'dateEff', 'dateExp']]

    def _ouput_formatting(self, df):
        df.columns = ['rtlSpId','materialCode',	'startDate','endDate']
        df['startDate'] = df['startDate'].dt.strftime("%Y/%m/%d")
        df['endDate'] = df['endDate'].dt.strftime("%Y/%m/%d")
        return df

    def process(self,target_date):
        self._date_formatting()
        promotion_data = self._promote_score(target_date)
        promotion_data = self._ouput_formatting(promotion_data)
        return promotion_data


    def get_promotion_info(self, target_date, store, sku):
        promotion_data = self.promote_score(target_date)
        result = promotion_data[(promotion_data['门店编码'] == store) & (promotion_data['规格'] == sku)]
        if result.empty:
            return 0
        else:
            return 1

    def get_promotion_info_df(self, df):
        merged = df.merge(self.data, left_on=['STORE_CODE', 'SAP_PROD_CODE'], right_on=['门店编码', '规格'], how='left')

        # 使用条件索引来检查df1中的'c'列日期是否在df2的'e'和'f'列日期之间件，就将'g'列设置为1
        merged['promotion'] = ((merged['ORDER_DATE2'] >= merged['有效期开始时间']) & (merged['ORDER_DATE2'] <= merged['有效期截止时间'])).astype(int)
        merged = merged.sort_values('promotion', ascending=False).drop_duplicates(subset=['STORE_CODE', 'SAP_PROD_CODE','ORDER_DATE2' ], keep='first')

        # 对于df1中的每一行，如果有多个匹配的行，只要有一个满足条
        df = df.merge(merged[['STORE_CODE', 'SAP_PROD_CODE','ORDER_DATE2', 'promotion']], on = ['STORE_CODE', 'SAP_PROD_CODE','ORDER_DATE2' ], how='left')
        # print(merged[(merged['STORE_CODE']=='78001034') & (merged['SAP_PROD_CODE']=='543205')])
        return df

if __name__ == "__main__":
    ### 用s3文件替换###
    PROMOTION_PATH = '.\Data\coupon.csv'
    coupon = pd.read_csv(PROMOTION_PATH, dtype={'storeCode':str, 'productCode':str})

    promotion = PromotionScore(coupon)
    date_format = "%Y/%m/%d"
    target_date = get_date_str()
    target_date = datetime.strptime(target_date, date_format)
    promotion_data = promotion.process(target_date)


    # 全部结果
    OUTPUT_DIR = '.\Result\\' + get_date_str('%Y%m%d')
    OUT_FILENAME_TOTAL = 'ai-promotion.csv'
    OUT_PATH_TOTAL = os.path.join(OUTPUT_DIR, OUT_FILENAME_TOTAL)
    promotion_data.to_csv(OUT_PATH_TOTAL, index=False)


    # print(promotion.get_promotion_info(target_date, '78001034', '543205'))



"""# 合并coupon

def coupon_extend(data):
    # 规格展开
    data['规格'] = data['规格'].str.split('/')
    data = data.explode('规格')
    return data
    
COUPON_COUNTRY_PATH = '..\Data/coupon.csv'
COUPON_SINGLE_PATH = '..\Data/coupon_single.csv'
df1 = pd.read_csv(COUPON_COUNTRY_PATH)
df2 = pd.read_csv(COUPON_SINGLE_PATH)
result = pd.concat([df1, df2])
result = coupon_extend(result)
result['有效期截止时间'] = pd.to_datetime(result['有效期截止时间'],format='ISO8601')

# 将日期格式化为你想要的格式
result['有效期截止时间'] = result['有效期截止时间'].dt.strftime('%Y/%m/%d')
result['有效期开始时间'] = pd.to_datetime(result['有效期开始时间'],format='ISO8601')

# 将日期格式化为你想要的格式
result['有效期开始时间'] = result['有效期开始时间'].dt.strftime('%Y/%m/%d')
print("不限SKU" in result['规格'].unique())
print(result.shape)
result.to_csv('..\Data/coupon_combined.csv', encoding='utf-8')"""