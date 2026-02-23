#!/usr/bin/env python3
"""
基金净值数据抓取脚本 - 修复版
从东方财富抓取真实净值数据
"""

import sqlite3
import requests
import re
import time
from datetime import datetime
from html import unescape

DB_PATH = '/root/.openclaw/workspace/fund_robot.db'

FUNDS = [
    {'code': '562500', 'name': '华夏中证机器人ETF', 'company': '华夏基金', 'type': 'ETF'},
    {'code': '159530', 'name': '易方达国证机器人产业ETF', 'company': '易方达基金', 'type': 'ETF'},
    {'code': '159526', 'name': '嘉实中证机器人ETF', 'company': '嘉实基金', 'manager': '田光远', 'type': 'ETF'},
    {'code': '159258', 'name': '南方中证机器人ETF', 'company': '南方基金', 'type': 'ETF'},
    {'code': '018095', 'name': '博时中证机器人指数发起C', 'company': '博时基金', 'manager': '唐屹兵', 'type': 'ETF'},
    {'code': '159559', 'name': '景顺长城国证机器人产业ETF', 'company': '景顺长城', 'type': 'ETF'},
    {'code': '159278', 'name': '鹏华国证机器人产业ETF', 'company': '鹏华基金', 'manager': '陈龙', 'type': 'ETF'},
    {'code': '159213', 'name': '汇添富中证机器人ETF', 'company': '汇添富基金', 'type': 'ETF'},
    {'code': '007713', 'name': '华富科技动能混合A', 'company': '华富基金', 'manager': '沈成', 'type': '主动管理'},
    {'code': '000649', 'name': '长城久鑫灵活配置混合A', 'company': '长城基金', 'manager': '余欢', 'type': '主动管理'},
    {'code': '021489', 'name': '中航趋势领航混合发起A', 'company': '中航基金', 'manager': '王森', 'type': '主动管理'},
    {'code': '018124', 'name': '永赢先进制造智选混合发起A', 'company': '永赢基金', 'manager': '张璐', 'type': '主动管理'},
]

def init_db():
    print("[步骤1/14] 初始化数据库...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS funds (
            fund_code TEXT PRIMARY KEY,
            fund_name TEXT NOT NULL,
            fund_company TEXT,
            fund_manager TEXT,
            fund_type TEXT,
            theme TEXT DEFAULT '机器人'
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS nav_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fund_code TEXT NOT NULL,
            date DATE NOT NULL,
            nav_value REAL,
            cumulative_nav REAL,
            daily_return REAL,
            UNIQUE(fund_code, date)
        )
    ''')
    
    conn.commit()
    conn.close()
    print("[步骤1/14] ✓ 数据库初始化完成")

def insert_funds():
    print("[步骤2/14] 插入基金基础信息...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    for fund in FUNDS:
        cursor.execute('''
            INSERT OR REPLACE INTO funds (fund_code, fund_name, fund_company, fund_manager, fund_type)
            VALUES (?, ?, ?, ?, ?)
        ''', (fund['code'], fund['name'], fund.get('company', ''), fund.get('manager', ''), fund['type']))
    
    conn.commit()
    conn.close()
    print(f"[步骤2/14] ✓ 已插入 {len(FUNDS)} 只基金信息")

def fetch_nav_eastmoney(fund_code):
    """从东方财富抓取基金净值"""
    url = f'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={fund_code}&page=1&per=1000'
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        resp = requests.get(url, headers=headers, timeout=30)
        resp.encoding = 'utf-8'
        
        content = resp.text
        
        # 提取表格内容
        # 格式: var apidata={ content:"<table>...</table>", records:1000, pages:1, curpage:1 };
        match = re.search(r'content:"([^"]+)"', content)
        if not match:
            return []
        
        html = match.group(1)
        # 解码HTML实体
        html = unescape(html)
        
        # 提取行数据
        nav_data = []
        row_pattern = r'<tr><td>(\d{4}-\d{2}-\d{2})</td><td[^>]*>([\d.]+)</td><td[^>]*>([\d.]+)</td><td[^>]*>([-\d.%]+)</td>'
        
        rows = re.findall(row_pattern, html)
        for row in rows:
            date_str, nav, cumulative_nav, daily_return = row
            
            # 清理日增长率
            daily_return = daily_return.replace('%', '').strip()
            if daily_return == '--':
                daily_return = None
            
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
        
    except Exception as e:
        print(f"    [错误] 抓取失败: {e}")
        return []

def save_nav(fund_code, nav_data):
    """保存净值到数据库"""
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
        except:
            pass
    
    conn.commit()
    conn.close()
    return count

def main():
    print("="*60)
    print("基金净值数据抓取脚本 - 真实数据版")
    print(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    init_db()
    time.sleep(0.5)
    
    insert_funds()
    time.sleep(0.5)
    
    total = len(FUNDS)
    for i, fund in enumerate(FUNDS, 3):
        code = fund['code']
        name = fund['name']
        
        print(f"[步骤{i}/{total+2}] 正在抓取: {name} ({code})")
        
        nav_data = fetch_nav_eastmoney(code)
        count = save_nav(code, nav_data)
        
        if count > 0:
            print(f"[步骤{i}/{total+2}] ✓ {name}: 成功导入 {count} 条真实净值记录")
        else:
            print(f"[步骤{i}/{total+2}] ✗ {name}: 未获取到数据")
        
        time.sleep(1)  # 避免请求过快
    
    print("\n" + "="*60)
    print("✓ 抓取完成 - 全部使用真实数据")
    print(f"数据库路径: {DB_PATH}")
    print("="*60)

if __name__ == '__main__':
    main()
