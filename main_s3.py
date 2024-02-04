pip install boto3
import pandas as pd
from datetime import datetime
import os
from promotion_score import PromotionScore
from product_score import ProductScore
from carparc_score_new import CarparcScore
from history_order_score import HistoryOrder
from total_score import TotalScore
from utils import get_date_str, score_output, filter_cooper, read_from_s3, order_data_selection, write_s3_files


def run():
    '''-----------------Read Files-------------------------'''
    ### 用s3文件替换###
    PROMOTION_PATH = 'ai/coupon/'
    coupon = read_from_s3(PROMOTION_PATH, dtype={'storeCode': str, 'productCode': str})

    ### 用s3文件替换###
    PRODUCT_PATH = 'ai/product/'
    product_info = read_from_s3(PRODUCT_PATH, dtype={'materialCode': str})
    cp_list = product_info[product_info['brand'] == 'CP']['materialCode'].values.tolist()

    OE_SKU_PATH = '.\Data/OE_SKU.csv'
    OE_SKU = pd.read_csv(OE_SKU_PATH)

    RP_PATH = '.\Data/ReplacementMarketSampleWithFuel.csv'
    rp = pd.read_csv(RP_PATH)

    ### 用s3文件替换###
    ORDER_DIR = 'ai/order/'
    order_data = read_from_s3(ORDER_DIR,  dtype={'storeCode':str, 'sapProdCode':str})
    order_data = order_data_selection(order_data)

    # 合并门店
    STORE_PATH = 'Data/history_store_revised.csv'
    store_data = pd.read_csv(STORE_PATH, usecols=['客户编号', '市'], dtype={'客户编号': str})

    ### 用s3文件替换###
    STORE_PATH_NEW = '.\Data\store.csv'
    store_data_new = read_from_s3(STORE_PATH_NEW, dtype={'客户编号': str}, cols=['客户编号', '市'])

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
    directory_name = get_date_str('%Y%m%d')

    # promotion
    promotion_data = filter_cooper(promotion_data,cp_list)
    write_s3_files(promotion_data, directory_name, 'ai-promotion.csv')

    # product
    product_score = filter_cooper(product_score, cp_list)
    product_score = product_score.sort_values('score',ascending=False)
    write_s3_files(product_score, directory_name, 'ai-product.csv')

    # carparc
    write_s3_files(new_rp, directory_name, 'ai-car-parc-total.csv' )
    # 前100
    new_rp = filter_cooper(new_rp,cp_list)
    new_rp_selected = score_output(new_rp, 'score', 100)
    write_s3_files(new_rp_selected, directory_name, 'ai-car-parc.csv')

    # history_order
    write_s3_files(history_score, directory_name, 'ai-historical-behavior-total.csv')
    history_score = filter_cooper(history_score, cp_list)
    history_score_selected = score_output(history_score, 'score', 100)
    write_s3_files(history_score_selected, directory_name, 'ai-historical-behavior.csv')

    # total
    write_s3_files(total_score, directory_name, 'ai-recommend-total.csv')
    total_score = filter_cooper(total_score, cp_list)
    total_score_selected = score_output(total_score, 'score', 100)
    write_s3_files(total_score_selected, directory_name, 'ai-recommend.csv')

run()