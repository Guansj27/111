import pandas as pd
from utils import read_order_data, get_date_str, score_output, replace_values
import datetime
import os

class CarparcScore:

    def __init__(self, data, product_data, store_data):
        self.data = data
        self.product_data = product_data
        self.store_data = store_data

    def _select_group(self):
        LUX = ['Luxury SUV',
               'Luxury Sports',
               'Luxury Passenger',
               'Mid SUV'
               ]

        EV = ['Luxury SUV',
              'Luxury Sports',
              'Luxury Passenger',
              'Mid SUV',
              'Mid Passenger']

        rp1 = self.data[self.data['16_BOX'].isin(LUX)]
        rp2 = self.data[(self.data['16_BOX'].isin(EV)) & (self.data['FUEL_TYPE'] != 'ICE')]
        new_rp = pd.concat([rp1, rp2])

        # sum without overlap
        new_rp.drop_duplicates(inplace=True)
        new_rp.drop(columns=['FUEL_TYPE', '16_BOX'], inplace=True)
        new_rp = new_rp.groupby(['PROVINCE', 'CITY', 'TIRESIZE', 'YEAR'])['REPLACEMENT_MARKET'].sum().reset_index()
        return new_rp

    def _map_with_sku(self, new_rp):
        new_rp = new_rp.merge(self.product_data[['materialCode', 'tyreSize']], left_on='TIRESIZE', right_on='tyreSize',
                              how='left')
        new_rp.dropna(subset=['materialCode'], inplace=True)
        return new_rp

    def calculate_carparc_score(self, year):
        self.data = self.data[self.data.YEAR == year]
        new_rp = self._select_group()
        new_rp = self._map_with_sku(new_rp)
        new_rp = new_rp.merge(self.store_data, left_on='CITY', right_on='市', how='left')
        return new_rp[['客户编号', 'materialCode', 'REPLACEMENT_MARKET']]

    def get_carparc_score(self, df):
        df['YEAR'] = df['orderDate2'].dt.year.astype(int)
        new_df = pd.DataFrame()
        for year in df['YEAR'].unique():
            sub_df = df[df.YEAR == year]
            carparc_score = self.calculate_carparc_score(year)
            sub_df = sub_df.merge(carparc_score, left_on=['storeCode', 'sapProdCode'], right_on=['客户编号', 'materialCode'], how='left')
            new_df = pd.concat([sub_df,new_df])
        new_df.drop(columns=['客户编号', 'materialCode','YEAR'], inplace=True)
        return new_df

    def ouput_formatting(self, df):
        df.columns = ['rtlSpId','materialCode',	'score']
        df['score'] = df['score'].round(6)
        return df



if __name__ == "__main__":
    RP_PATH = '..\Data/ReplacementMarketSampleWithFuel.csv'
    rp = pd.read_csv(RP_PATH)

    ### 用s3文件替换###
    PRODUCT_PATH = '.\Data\product.csv'
    product_info = pd.read_csv(PRODUCT_PATH, dtype={'materialCode':str})

    # 合并门店
    STORE_PATH = '..\Data/门店基础信息_revised.csv'
    store_data = pd.read_csv(STORE_PATH, usecols=['客户编号', '市'], dtype={'客户编号': str})

    ### 用s3文件替换###
    STORE_PATH_NEW = '.\Data\store.csv'
    store_data_new = pd.read_csv(STORE_PATH_NEW, usecols=['客户编号', '省', '市'], dtype={'客户编号': str})
    store_data_new = replace_values(store_data_new)

    #合并两个数据源
    store_data_concat = pd.concat([store_data, store_data_new], ignore_index=True)
    store_data_concat = store_data_concat.drop_duplicates(subset='客户编号', keep='first')
    store_data_concat = store_data_concat[store_data_concat['客户编号'].str.startswith('7')]

    carparc = CarparcScore(rp, product_info, store_data_concat)

    # 获取年份
    current_year = datetime.datetime.now().year
    new_rp = carparc.calculate_carparc_score(current_year)

    new_rp = carparc.ouput_formatting(new_rp)

    # 全部结果
    OUTPUT_DIR = '.\Result\\' +  get_date_str('%Y%m%d')
    OUT_FILENAME_TOTAL = 'ai-car-parc-total.csv'
    OUT_PATH_TOTAL = os.path.join(OUTPUT_DIR, OUT_FILENAME_TOTAL)
    new_rp.to_csv(OUT_PATH_TOTAL, index=False)

    # 前100
    new_rp_selected = score_output(new_rp, 'score', 100)
    OUT_FILENAME = 'ai-car-parc.csv'
    OUT_PATH = os.path.join(OUTPUT_DIR, OUT_FILENAME)
    new_rp_selected.to_csv(OUT_PATH, index=False)

