import pandas as pd
from datetime import datetime
import os
from promotion_score import PromotionScore
from product_score import ProductScore
from carparc_score_new import CarparcScore
from history_order_score import HistoryOrder
from total_score import TotalScore
from utils import get_date_str, read_order_data, score_output, filter_cooper


def my_function():
    # 你的代码
    '''-----------------Read Files-------------------------'''
    ### 用s3文件替换###
    PROMOTION_PATH = './Data/coupon.csv'
    coupon = pd.read_csv(PROMOTION_PATH, dtype={'storeCode': str, 'productCode': str})

    ### 用s3文件替换###
    PRODUCT_PATH = './Data/product.csv'
    product_info = pd.read_csv(PRODUCT_PATH, dtype={'materialCode': str})
    cp_list = product_info[product_info['brand'] == 'CP']['materialCode'].values.tolist()
    print(cp_list)

    OE_SKU_PATH = './Data/OE_SKU.csv'
    OE_SKU = pd.read_csv(OE_SKU_PATH)

    RP_PATH = './Data/ReplacementMarketSampleWithFuel.csv'
    rp = pd.read_csv(RP_PATH)

    ### 用s3文件替换###
    ORDER_DIR = './Data/order'
    order_data = read_order_data(ORDER_DIR)

    # 合并门店
    STORE_PATH = './Data/history_store_revised.csv'
    store_data = pd.read_csv(STORE_PATH, usecols=['客户编号', '市'], dtype={'客户编号': str})

    ### 用s3文件替换###
    STORE_PATH_NEW = './Data/store.csv'
    store_data_new = pd.read_csv(STORE_PATH_NEW, usecols=['客户编号', '市'], dtype={'客户编号': str})

    # 合并两个数据源
    store_data_concat = pd.concat([store_data, store_data_new], ignore_index=True)
    store_data_concat = store_data_concat.drop_duplicates(subset='客户编号', keep='first')
    store_data_concat = store_data_concat[store_data_concat['客户编号'].str.startswith('7')]

    # 获取年份
    current_year = datetime.now().year

    # 获取今天的日期：str
    current_date = get_date_str()
    date_format = "%Y/%m/%d"
    current_date_dt = datetime.strptime(current_date, date_format)

    '''----------------Promotion------------------------'''
    promotion = PromotionScore(coupon)
    promotion_data = promotion.process(current_date_dt)

    '''----------------Product---------------------------'''
    product = ProductScore(product_info, OE_SKU)
    product_score = product.calculate_product_score(current_year)
    product_score = product.ouput_formatting(product_score)

    '''---------------Carparc----------------------------'''
    carparc = CarparcScore(rp, product_info, store_data_concat)
    new_rp = carparc.calculate_carparc_score(current_year)
    new_rp = carparc.ouput_formatting(new_rp)

    '''---------------HistoryOrder------------------------'''
    history_order = HistoryOrder(order_data)
    history_score = history_order.total_score(current_date)
    history_score = history_order.ouput_formatting(history_score)

    '''----------------TotalScore-------------------------'''
    total = TotalScore(product_score, promotion_data, new_rp, history_score)
    total_score = total.process()

    '''----------------Output-----------------------------'''
    OUTPUT_DIR = '.\Result\\' + get_date_str('%Y%m%d')

    OUT_FILENAME_TOTAL = 'ai-promotion.csv'
    OUT_PATH_TOTAL = os.path.join(OUTPUT_DIR, OUT_FILENAME_TOTAL)
    promotion_data = filter_cooper(promotion_data, cp_list)
    promotion_data.to_csv(OUT_PATH_TOTAL, index=False)

    OUT_FILENAME_TOTAL = 'ai-product.csv'
    OUT_PATH_TOTAL = os.path.join(OUTPUT_DIR, OUT_FILENAME_TOTAL)
    product_score = filter_cooper(product_score, cp_list)
    product_score.sort_values('score', ascending=False).to_csv(OUT_PATH_TOTAL, index=False)

    OUT_FILENAME_TOTAL = 'ai-car-parc-total.csv'
    OUT_PATH_TOTAL = os.path.join(OUTPUT_DIR, OUT_FILENAME_TOTAL)
    new_rp.to_csv(OUT_PATH_TOTAL, index=False)

    # 前100
    new_rp = filter_cooper(new_rp, cp_list)
    new_rp_selected = score_output(new_rp, 'score', 100)
    OUT_FILENAME = 'ai-car-parc.csv'
    OUT_PATH = os.path.join(OUTPUT_DIR, OUT_FILENAME)
    new_rp_selected.to_csv(OUT_PATH, index=False)

    OUT_FILENAME_TOTAL = 'ai-historical-behavior-total.csv'
    OUT_PATH_TOTAL = os.path.join(OUTPUT_DIR, OUT_FILENAME_TOTAL)
    history_score.to_csv(OUT_PATH_TOTAL, index=False)

    history_score = filter_cooper(history_score, cp_list)
    history_score_selected = score_output(history_score, 'score', 100)

    OUT_FILENAME = 'ai-historical-behavior.csv'
    OUT_PATH = os.path.join(OUTPUT_DIR, OUT_FILENAME)

    history_score_selected.to_csv(OUT_PATH, index=False)

    OUT_FILENAME_TOTAL = 'ai-recommend-total.csv'
    OUT_PATH_TOTAL = os.path.join(OUTPUT_DIR, OUT_FILENAME_TOTAL)
    total_score.to_csv(OUT_PATH_TOTAL, index=False)

    total_score = filter_cooper(total_score, cp_list)
    total_score_selected = score_output(total_score, 'score', 100)

    OUT_FILENAME = 'ai-recommend.csv'
    OUT_PATH = os.path.join(OUTPUT_DIR, OUT_FILENAME)
    total_score_selected.to_csv(OUT_PATH, index=False)


if __name__ == '__main__':
    my_function()
