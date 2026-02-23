#!/usr/bin/env python3
"""
基金净值数据抓取脚本 - V3
增加增量抓取和进度记录
"""

import sqlite3
import requests
import re
import time
import sys
import os
from datetime import datetime
from html import unescape

DB_PATH = '/home/xiaoman/xiaoman/fund_scraper/fund_robot.db'

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

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    sys.stdout.flush()

def init_db():
    log("初始化数据库...")
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
            theme TEXT DEFAULT '机器人'
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
            UNIQUE(fund_code, date)
        )
    ''')
    
    # 同步进度表（新增）
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sync_meta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            current_fund_code TEXT,
            current_fund_name TEXT,
            current_page INTEGER DEFAULT 1,
            current_fund_index INTEGER DEFAULT 0,
            total_funds INTEGER DEFAULT 12,
            status TEXT DEFAULT 'running',
            last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # 初始化或更新同步记录
    cursor.execute('SELECT COUNT(*) FROM sync_meta')
    if cursor.fetchone()[0] == 0:
        cursor.execute('''
            INSERT INTO sync_meta (current_fund_code, current_fund_name, current_page, current_fund_index, total_funds, status)
            VALUES (?, ?, 1, 0, 12, 'running')
        ''', (FUNDS[0]['code'], FUNDS[0]['name']))
    
    conn.commit()
    conn.close()
    log("✓ 数据库初始化完成")

def get_sync_status():
    """获取当前同步状态"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM sync_meta ORDER BY id DESC LIMIT 1')
    row = cursor.fetchone()
    conn.close()
    
    if row:
        return {
            'current_fund_code': row[1],
            'current_fund_name': row[2],
            'current_page': row[3],
            'current_fund_index': row[4],
            'total_funds': row[5],
            'status': row[6]
        }
    return None

def update_sync_status(fund_code, fund_name, page, fund_index, status='running'):
    """更新同步状态"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE sync_meta SET 
            current_fund_code = ?, 
            current_fund_name = ?, 
            current_page = ?, 
            current_fund_index = ?,
            status = ?,
            last_update = CURRENT_TIMESTAMP
        WHERE id = (SELECT id FROM sync_meta ORDER BY id DESC LIMIT 1)
    ''', (fund_code, fund_name, page, fund_index, status))
    conn.commit()
    conn.close()

def insert_funds():
    log("插入基金基础信息...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    for fund in FUNDS:
        cursor.execute('''
            INSERT OR REPLACE INTO funds (fund_code, fund_name, fund_company, fund_manager, fund_type)
            VALUES (?, ?, ?, ?, ?)
        ''', (fund['code'], fund['name'], fund.get('company', ''), fund.get('manager', ''), fund['type']))
    
    conn.commit()
    conn.close()
    log(f"✓ 已插入 {len(FUNDS)} 只基金信息")

def fetch_nav_eastmoney_page(fund_code, page, retry=1):
    """抓取单页数据"""
    url = f'http://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code={fund_code}&page={page}&per=20'
    
    for attempt in range(retry + 1):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            resp = requests.get(url, headers=headers, timeout=30)
            resp.encoding = 'utf-8'
            
            content = resp.text
            
            if 'records:0' in content or 'content:""' in content:
                return None  # 无更多数据
            
            match = re.search(r'content:"([^"]*)"', content)
            if not match or not match.group(1):
                return None
            
            html = unescape(match.group(1))
            nav_data = []
            row_pattern = r'<tr><td>(\d{4}-\d{2}-\d{2})</td><td[^>]*>([\d.]+)</td><td[^>]*>([\d.]+)</td><td[^>]*>([-\d.%]+)</td>'
            rows = re.findall(row_pattern, html)
            
            for row in rows:
                date_str, nav, cumulative_nav, daily_return = row
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
            if attempt < retry:
                time.sleep(3)
            else:
                return None

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
    log("="*60)
    log("基金净值数据抓取脚本 V3 - 增量抓取")
    log(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("="*60)
    
    init_db()
    insert_funds()
    
    # 获取同步状态
    sync = get_sync_status()
    start_index = sync['current_fund_index'] if sync else 0
    start_page = sync['current_page'] if sync else 1
    
    log(f"从第{start_index+1}只基金、第{start_page}页继续抓取...")
    
    total = len(FUNDS)
    
    for i in range(start_index, total):
        fund = FUNDS[i]
        code = fund['code']
        name = fund['name']
        
        log(f"[{i+1}/{total}] {name}")
        
        # 从记录的页码开始，或从第1页开始
        page = start_page if i == start_index else 1
        fund_total = 0
        
        while True:
            update_sync_status(code, name, page, i, 'running')
            
            nav_data = fetch_nav_eastmoney_page(code, page, retry=1)
            
            if not nav_data:
                log(f"  第{page}页无数据，基金完成")
                break
            
            count = save_nav(code, nav_data)
            fund_total += count
            log(f"  第{page}页: {count}条")
            
            # 检查是否还有下一页
            if len(nav_data) < 20:
                break
            
            page += 1
            time.sleep(1)  # 页间延时
        
        log(f"  ✓ {name}: 共{fund_total}条")
        
        # 重置下一基金从第1页开始
        start_page = 1
        
        time.sleep(2)  # 基金间延时
    
    # 标记完成
    update_sync_status('', '', 1, total, 'completed')
    
    log("\n" + "="*60)
    log("✓ 所有基金抓取完成")
    log(f"数据库: {DB_PATH}")
    log("="*60)

if __name__ == '__main__':
    main()
