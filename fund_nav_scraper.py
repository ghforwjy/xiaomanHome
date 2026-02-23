#!/usr/bin/env python3
"""
基金净值数据抓取脚本
抓取机器人主题基金的历史净值数据并存入SQLite数据库
"""

import sqlite3
import requests
import json
import time
from datetime import datetime, timedelta
from pathlib import Path

# 数据库路径
DB_PATH = '/root/.openclaw/workspace/fund_robot.db'

# 基金列表
FUNDS = [
    # ETF
    {'code': '562500', 'name': '华夏中证机器人ETF', 'company': '华夏基金', 'manager': '-', 'type': 'ETF'},
    {'code': '159530', 'name': '易方达国证机器人产业ETF', 'company': '易方达基金', 'manager': '-', 'type': 'ETF'},
    {'code': '159526', 'name': '嘉实中证机器人ETF', 'company': '嘉实基金', 'manager': '田光远', 'type': 'ETF'},
    {'code': '159258', 'name': '南方中证机器人ETF', 'company': '南方基金', 'manager': '-', 'type': 'ETF'},
    {'code': '018095', 'name': '博时中证机器人指数发起C', 'company': '博时基金', 'manager': '唐屹兵', 'type': 'ETF'},
    {'code': '159559', 'name': '景顺长城国证机器人产业ETF', 'company': '景顺长城', 'manager': '-', 'type': 'ETF'},
    {'code': '159278', 'name': '鹏华国证机器人产业ETF', 'company': '鹏华基金', 'manager': '陈龙', 'type': 'ETF'},
    {'code': '159213', 'name': '汇添富中证机器人ETF', 'company': '汇添富基金', 'manager': '-', 'type': 'ETF'},
    
    # 主动管理型
    {'code': '007713', 'name': '华富科技动能混合A', 'company': '华富基金', 'manager': '沈成', 'type': '主动管理'},
    {'code': '000649', 'name': '长城久鑫灵活配置混合A', 'company': '长城基金', 'manager': '余欢', 'type': '主动管理'},
    {'code': '021489', 'name': '中航趋势领航混合发起A', 'company': '中航基金', 'manager': '王森', 'type': '主动管理'},
    {'code': '018124', 'name': '永赢先进制造智选混合发起A', 'company': '永赢基金', 'manager': '张璐', 'type': '主动管理'},
]

def init_database():
    """初始化数据库"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # 基金基础信息表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS funds (
            fund_code TEXT PRIMARY KEY,
            fund_name TEXT NOT NULL,
            fund_company TEXT,
            fund_manager TEXT,
            fund_type TEXT,
            theme TEXT DEFAULT '机器人',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 净值历史表
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS nav_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code TEXT NOT NULL,
            date DATE NOT NULL,
            nav_value REAL,
            cumulative_nav REAL,
            daily_return REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fund_code, date),
            FOREIGN KEY (fund_code) REFERENCES funds(fund_code)
        )
    ''')
    
    # 创建索引
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_nav_fund_date ON nav_history(fund_code, date)')
    
    conn.commit()
    conn.close()
    print("[INFO] 数据库初始化完成")

def insert_fund_info():
    """插入基金基础信息"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    for fund in FUNDS:
        cursor.execute('''
            INSERT OR REPLACE INTO funds (fund_code, fund_name, fund_company, fund_manager, fund_type)
            VALUES (?, ?, ?, ?, ?)
        ''', (fund['code'], fund['name'], fund['company'], fund['manager'], fund['type']))
    
    conn.commit()
    conn.close()
    print(f"[INFO] 已插入 {len(FUNDS)} 只基金基础信息")

def fetch_nav_from_eastmoney(fund_code):
    """从东方财富抓取基金净值数据"""
    url = f'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={fund_code}&page=1&per=10000'
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.encoding = 'utf-8'
        
        # 解析返回的数据
        content = response.text
        
        # 提取JSON数据
        if 'content":"' in content:
            # 解析HTML表格
            import re
            from html import unescape
            
            # 提取表格行
            pattern = r'<tr>(.*?)</tr>'
            rows = re.findall(pattern, content, re.DOTALL)
            
            nav_data = []
            for row in rows[1:]:  # 跳过表头
                cells = re.findall(r'<td>(.*?)</td>', row)
                if len(cells) >= 4:
                    date_str = cells[0].strip()
                    nav = cells[1].strip()
                    cumulative_nav = cells[2].strip()
                    daily_return = cells[3].strip().replace('%', '')
                    
                    try:
                        nav_data.append({
                            'date': date_str,
                            'nav': float(nav) if nav else None,
                            'cumulative_nav': float(cumulative_nav) if cumulative_nav else None,
                            'daily_return': float(daily_return) if daily_return else None
                        })
                    except:
                        pass
            
            return nav_data
        
        return []
    except Exception as e:
        print(f"[ERROR] 抓取基金 {fund_code} 失败: {e}")
        return []

def fetch_nav_from_sina(fund_code):
    """从新浪财经抓取基金净值数据（备用）"""
    url = f'http://stock.finance.sina.com.cn/fundInfo/view/FundInfo_LSJZ.php?symbol={fund_code}'
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.encoding = 'gb2312'
        
        import re
        from html import unescape
        
        content = response.text
        
        # 提取表格数据
        pattern = r'<tr[^>]*>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>'
        matches = re.findall(pattern, content, re.DOTALL)
        
        nav_data = []
        for match in matches:
            date_str = re.sub(r'<[^>]+>', '', match[0]).strip()
            nav = re.sub(r'<[^>]+>', '', match[1]).strip()
            cumulative_nav = re.sub(r'<[^>]+>', '', match[2]).strip()
            daily_return = re.sub(r'<[^>]+>', '', match[3]).strip().replace('%', '')
            
            try:
                if date_str and '/' in date_str:
                    # 转换日期格式 2025/12/31 -> 2025-12-31
                    parts = date_str.split('/')
                    date_str = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                
                nav_data.append({
                    'date': date_str,
                    'nav': float(nav) if nav else None,
                    'cumulative_nav': float(cumulative_nav) if cumulative_nav else None,
                    'daily_return': float(daily_return) if daily_return else None
                })
            except:
                pass
        
        return nav_data
    except Exception as e:
        print(f"[ERROR] 新浪财经抓取基金 {fund_code} 失败: {e}")
        return []

def save_nav_to_db(fund_code, nav_data):
    """保存净值数据到数据库"""
    if not nav_data:
        return 0
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    count = 0
    for item in nav_data:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO nav_history (fund_code, date, nav_value, cumulative_nav, daily_return)
                VALUES (?, ?, ?, ?, ?)
            ''', (fund_code, item['date'], item['nav'], item['cumulative_nav'], item['daily_return']))
            count += 1
        except Exception as e:
            print(f"[WARN] 插入数据失败 {fund_code} {item['date']}: {e}")
    
    conn.commit()
    conn.close()
    return count

def main():
    """主函数"""
    print("="*60)
    print("基金净值数据抓取脚本启动")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # 初始化数据库
    init_database()
    
    # 插入基金基础信息
    insert_fund_info()
    
    # 抓取每只基金的净值数据
    total_funds = len(FUNDS)
    for i, fund in enumerate(FUNDS, 1):
        fund_code = fund['code']
        fund_name = fund['name']
        
        print(f"\n[PROGRESS] ({i}/{total_funds}) 正在抓取: {fund_name} ({fund_code})")
        
        # 先尝试东方财富
        nav_data = fetch_nav_from_eastmoney(fund_code)
        
        # 如果失败，尝试新浪财经
        if not nav_data:
            print(f"[INFO] 东方财富无数据，尝试新浪财经...")
            nav_data = fetch_nav_from_sina(fund_code)
        
        if nav_data:
            count = save_nav_to_db(fund_code, nav_data)
            print(f"[SUCCESS] {fund_name}: 成功导入 {count} 条净值记录")
        else:
            print(f"[WARN] {fund_name}: 未获取到数据")
        
        # 延时，避免请求过快
        time.sleep(2)
    
    print("\n" + "="*60)
    print("抓取完成")
    print(f"数据库路径: {DB_PATH}")
    print("="*60)

if __name__ == '__main__':
    main()
