import pandas as pd
import numpy as np
import statsmodels.api as sm

# 生成一些随机数据
np.random.seed(123)
data = np.random.randn(100)

# 将数据转换为时间序列
dates = pd.date_range(start='2022-01-01', periods=len(data), freq='D')
ts = pd.Series(data, index=dates)

# 构建 ARIMA 模型
model = sm.tsa.ARIMA(ts, order=(7,1,0))

# 拟合模型并进行预测
result = model.fit()
forecast = result.forecast(steps=10)

# 打印预测结果
print(forecast)

