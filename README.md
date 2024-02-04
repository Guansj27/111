# Order Recommendation

## 1. 文件说明
 - Data
   - order
     - order.csv - 过去一年订单
     - order-history.csv - 一年前订单
   - coupon.csv - RTM优惠信息
   - history_store_revised.csv - 门店信息
   - OE_SKU.csv - OE规格SKU
   - product.csv - 产品信息
   - ReplacementMarketSampleWithFuel.csv - ReplacementMarket

## 2. 运行Memo
本地***main.py*** \
aws ***main_s3.py***
-  权重调整: 目前所有因子权重均为0.25，如需调整权重，则修改***total_score.py***中weights
- ReplacementMarket暂时获取不到2024年数据，沿用2023年数据，2024数据更新后需要替换文件ReplacementMarketSampleWithFuel.csv

## 3. 优化方向
1. history_order_score中加入门店特征、商品特征、用户行为埋点数据等，如点击，加购
2. 针对不同地区/用户等级（Z01、Z02）设置不同规则，可以通过A/B test测试不同权重的影响
3. promotion根据不同优惠力度有不同分数
4. 结合库存信息
5. 每个sku推荐不同的条数