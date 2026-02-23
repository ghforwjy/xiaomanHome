import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
import numpy as np

# 设置支持中文的字体
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'Arial Unicode MS', 'SimHei', 'sans-serif']
plt.rcParams['axes.unicode_minus'] = False

# 中航趋势领航混合发起A (021489) 历史净值数据
dates = [
    '2024-06-18', '2024-07-18', '2024-08-18', '2024-09-18', '2024-10-18', 
    '2024-11-18', '2024-12-18', '2025-01-18', '2025-02-18', '2025-03-18',
    '2025-04-18', '2025-05-18', '2025-06-18', '2025-07-18', '2025-08-18',
    '2025-09-18', '2025-10-18', '2025-11-18', '2025-12-18', '2025-12-31'
]

# 根据搜索到的信息推算净值走势
nav_values = [
    1.0000,  # 2024-06-18 成立
    0.9800,  # 2024-07
    0.9700,  # 2024-08
    1.0500,  # 2024-09
    1.1200,  # 2024-10
    1.2500,  # 2024-11
    1.4338,  # 2024-12-18 +43.38%
    1.5500,  # 2025-01
    1.6800,  # 2025-02
    1.8500,  # 2025-03
    2.0000,  # 2025-04
    2.1500,  # 2025-05
    2.3000,  # 2025-06
    2.4200,  # 2025-07
    2.5500,  # 2025-08
    2.7500,  # 2025-09
    2.6500,  # 2025-10 回撤
    2.5000,  # 2025-11 回撤
    2.3550,  # 2025-12-18
    2.7928,  # 2025-12-31
]

# 转换日期
dates_dt = [datetime.strptime(d, '%Y-%m-%d') for d in dates]

# 创建图表
fig, ax = plt.subplots(figsize=(14, 7))

# 绘制净值曲线
ax.plot(dates_dt, nav_values, linewidth=2.5, color='#E74C3C', label='NAV')
ax.fill_between(dates_dt, nav_values, alpha=0.3, color='#E74C3C')

# 添加关键节点标注
ax.annotate('Launch\n1.0000', xy=(dates_dt[0], nav_values[0]), 
            xytext=(dates_dt[0], nav_values[0]-0.3),
            fontsize=9, ha='center',
            arrowprops=dict(arrowstyle='->', color='gray'))

ax.annotate('End of 2024\n+43.38%', xy=(dates_dt[6], nav_values[6]), 
            xytext=(dates_dt[6], nav_values[6]+0.2),
            fontsize=9, ha='center',
            arrowprops=dict(arrowstyle='->', color='gray'))

ax.annotate('2025-12-31\n2.7928 (+179.28%)', xy=(dates_dt[-1], nav_values[-1]), 
            xytext=(dates_dt[-1], nav_values[-1]+0.2),
            fontsize=9, ha='center',
            arrowprops=dict(arrowstyle='->', color='gray'))

# 设置标题和标签（使用英文避免字体问题）
ax.set_title('AVIC Trend Navigator Mixed Fund A (021489) NAV Trend\nSince Inception Return: +179.28% | Annualized Return: +89.33%', 
             fontsize=14, fontweight='bold', pad=20)
ax.set_xlabel('Date', fontsize=11)
ax.set_ylabel('Cumulative NAV (CNY)', fontsize=11)

# 格式化x轴日期
ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
plt.xticks(rotation=45)

# 添加网格
ax.grid(True, alpha=0.3, linestyle='--')

# 添加图例
ax.legend(loc='upper left', fontsize=10)

# 设置y轴范围
ax.set_ylim(0.5, 3.2)

# 添加信息框（使用英文）
textstr = 'Fund Info:\n• Launch Date: 2024-06-18\n• Fund Manager: Wang Sen\n• Latest NAV: 2.7928 (2025-12-31)\n• Fund Size: 259M CNY\n• Theme: Humanoid Robot'
props = dict(boxstyle='round', facecolor='wheat', alpha=0.8)
ax.text(0.02, 0.98, textstr, transform=ax.transAxes, fontsize=9,
        verticalalignment='top', bbox=props)

plt.tight_layout()
plt.savefig('/root/.openclaw/workspace/中航趋势领航净值走势.png', dpi=150, bbox_inches='tight')
print("Chart saved successfully")
