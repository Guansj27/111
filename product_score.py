import warnings
warnings.filterwarnings('ignore')

import pandas as pd
import os
from utils import get_date_str

class ProductScore:

    def __init__(self, data, OE_SKU):
        self.data = data
        self.data['product_score'] = 0
        self.OE_SKU = OE_SKU

    def _inch(self):
        self.data['product_score'] = self.data.apply(lambda row: row['product_score'] + 1 if row['rim'] > 17 else row['product_score'], axis=1)

    def _category(self):
        self.data['product_score'] = self.data['product_score'] + self.data[['suv', 'lux', 'ev']].apply(lambda row: (row == 1).sum(), axis=1)

    def _newproduct(self, year):
        self.data['product_score'] = self.data.apply(lambda row: row['product_score'] + 1 if row['yearOfNew'] == year else row['product_score'],axis=1)

    def _OE(self):
        self.data['product_score'] = self.data.apply(
            lambda row: row['product_score'] + 1 if row['materialCode'] in(self.OE_SKU.other_values.values) else row['product_score'], axis=1)

    def calculate_product_score(self, year):
        self._inch()
        self._category()
        self._newproduct(year)
        self._OE()
        self.data['product_score'] = self.data['product_score']/6
        return self.data[['materialCode', 'product_score']]

    def get_product_score(self, df):
        df['YEAR'] = df['orderDate2'].dt.year.astype(int)
        new_df = pd.DataFrame()
        for year in df['YEAR'].unique():
            sub_df = df[df.YEAR == year]
            product_score = self.calculate_product_score(year)
            sub_df = sub_df.merge(product_score, left_on='sapProdCode', right_on='materialCode', how='left')
            new_df = pd.concat([sub_df,new_df])
        new_df.drop(columns=['materialCode', 'YEAR'], inplace=True)
        return new_df

    def ouput_formatting(self, df):
        df.columns = ['materialCode',	'score']
        df['score'] = df['score'].round(6)
        return df

if __name__ == "__main__":
    ### 用s3文件替换###
    PRODUCT_PATH = '.\Data\product.csv'
    product_info = pd.read_csv(PRODUCT_PATH, dtype={'materialCode':str})

    OE_SKU_PATH = '..\Data/OE_SKU.csv'
    OE_SKU = pd.read_csv(OE_SKU_PATH)

    product = ProductScore(product_info, OE_SKU)
    product_score = product.calculate_product_score(2023)
    product_score = product.ouput_formatting(product_score)

    OUTPUT_DIR = '.\Result\\' + get_date_str('%Y%m%d')
    OUT_FILENAME_TOTAL = 'ai-product.csv'
    OUT_PATH_TOTAL = os.path.join(OUTPUT_DIR, OUT_FILENAME_TOTAL)
    product_score.sort_values('score',ascending=False).to_csv(OUT_PATH_TOTAL,index=False)
