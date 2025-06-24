import streamlit as st
import pandas as pd
import akshare as ak
import datetime
import time
import requests
import base64
import os
from dateutil.relativedelta import relativedelta

# 页面配置
st.set_page_config(
    page_title="全市场ETF期权贴水分析",
    layout="wide",
    initial_sidebar_state="expanded"
)

# GitHub配置
GITHUB_OWNER = "lennyshen"
GITHUB_REPO = "SSEOptions"
GITHUB_FILE_PATH = "All_SSE_ETF_Option_Premium_Log.csv"
GITHUB_TOKEN = st.secrets["GT"]

# 全局变量存储最新的计算结果
if 'latest_premium_data' not in st.session_state:
    st.session_state.latest_premium_data = None

# 从GitHub读取数据的函数
def read_data_from_github():
    """从GitHub仓库读取CSV数据"""
    try:
        url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            # 文件存在，获取内容
            file_info = response.json()
            content = base64.b64decode(file_info['content']).decode('utf-8-sig')
            
            # 从字符串创建DataFrame
            from io import StringIO
            df = pd.read_csv(StringIO(content))
            return df, file_info['sha']
        else:
            # 文件不存在，返回空的DataFrame
            return pd.DataFrame(), None
            
    except Exception as e:
        st.error(f"从GitHub读取数据时出错: {str(e)}")
        return pd.DataFrame(), None

# 保存数据到GitHub的函数
def save_data_to_github():
    """保存当前数据到GitHub仓库"""
    if st.session_state.latest_premium_data is None or st.session_state.latest_premium_data.empty:
        st.error("没有可保存的数据，请先运行数据获取")
        return False
    
    try:
        # 准备要保存的数据
        data_to_save = st.session_state.latest_premium_data.copy()
        
        # 对数值列进行4位小数格式化
        if '贴水价值' in data_to_save.columns:
            data_to_save['贴水价值'] = data_to_save['贴水价值'].round(4)
        if '年化贴水率' in data_to_save.columns:
            data_to_save['年化贴水率'] = data_to_save['年化贴水率'].round(4)
        if '行权价' in data_to_save.columns:
            data_to_save['行权价'] = data_to_save['行权价'].round(4)
        if '剩余天数' in data_to_save.columns:
            data_to_save['剩余天数'] = data_to_save['剩余天数'].astype(int)  # 只保留整数部分
        
        # 添加当前日期列
        current_date = datetime.date.today().strftime('%Y-%m-%d')
        data_to_save['记录日期'] = current_date
        
        # 重新排列列的顺序，将日期放在最后（保持与现有格式一致）
        columns_order = ['ETF类型', '合约月份', '行权价', '贴水价值', '年化贴水率', '剩余天数', '记录日期']
        data_to_save = data_to_save[columns_order]
        
        # 改进的ETF类型名称显示
        etf_display_names = {
            "华泰柏瑞沪深300ETF期权": "300ETF",
            "南方中证500ETF期权": "500ETF", 
            "华夏上证50ETF期权": "50ETF",
            "华夏科创50ETF期权": "科创50ETF",
            "易方达科创50ETF期权": "科创板50ETF"
        }
        
        # 替换ETF类型名称为简化版本
        data_to_save['ETF类型'] = data_to_save['ETF类型'].map(etf_display_names)
        
        # 从GitHub读取现有数据
        existing_data, sha = read_data_from_github()
        
        if not existing_data.empty:
            # 删除同日期的记录
            existing_data = existing_data[existing_data['记录日期'] != current_date]
            # 合并数据
            final_data = pd.concat([existing_data, data_to_save], ignore_index=True)
        else:
            # 没有现有数据，使用新数据
            final_data = data_to_save
        
        # 按日期排序
        final_data = final_data.sort_values('记录日期', ascending=False)
        
        # 将DataFrame转换为CSV字符串
        csv_content = final_data.to_csv(index=False, encoding='utf-8-sig')
        
        # 编码内容
        encoded_content = base64.b64encode(csv_content.encode('utf-8-sig')).decode()
        
        # 准备提交数据
        payload = {
            "message": f"Update {GITHUB_FILE_PATH} via API - {current_date}",
            "content": encoded_content
        }
        
        # 如果文件已存在，添加sha
        if sha:
            payload["sha"] = sha
        
        # 提交到GitHub
        url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/contents/{GITHUB_FILE_PATH}"
        headers = {"Authorization": f"token {GITHUB_TOKEN}"}
        response = requests.put(url, json=payload, headers=headers)
        
        if response.status_code in [200, 201]:
            st.success(f"✅ 数据已保存到GitHub仓库，共 {len(data_to_save)} 条记录")
            return True
        else:
            st.error(f"保存到GitHub失败: {response.status_code} - {response.json()}")
            return False
        
    except Exception as e:
        st.error(f"保存数据时出错: {str(e)}")
        return False

# 标题和说明
st.title("All SSE ETF Options Premium Dashboard")
st.markdown("""
本仪表板展示全部上交所ETF期权的贴水分析数据，每5分钟自动刷新一次。
数据将保存到GitHub仓库中。
""")

# 自动计算合约月份的函数
def get_contract_months():
    """根据第4个星期三规则自动计算合约月份"""
    today = datetime.date.today()
    
    # 计算本月第4个星期三
    first_day = datetime.date(today.year, today.month, 1)
    # 计算第一个星期三
    first_wednesday = first_day + datetime.timedelta(days=(2 - first_day.weekday()) % 7)
    # 第四个星期三 = 第一个星期三 + 3周
    fourth_wednesday = first_wednesday + datetime.timedelta(weeks=3)
    
    # 判断今天是否在本月第4个周三及之前
    if today <= fourth_wednesday:
        # 使用本月作为基准
        base_month = today.month
        base_year = today.year
    else:
        # 使用下月作为基准
        if today.month == 12:
            base_month = 1
            base_year = today.year + 1
        else:
            base_month = today.month + 1
            base_year = today.year
    
    # 计算4个合约月份
    contract_months = []
    
    # 本月合约
    current_month = f"{base_year % 100:02d}{base_month:02d}"
    contract_months.append(current_month)
    
    # 下月合约
    if base_month == 12:
        next_month = 1
        next_year = base_year + 1
    else:
        next_month = base_month + 1
        next_year = base_year
    next_month_contract = f"{next_year % 100:02d}{next_month:02d}"
    contract_months.append(next_month_contract)
    
    # 本季合约（3、6、9、12月）
    quarter_months = [3, 6, 9, 12]
    current_quarter_month = None
    current_quarter_year = base_year
    
    for qm in quarter_months:
        if base_month <= qm:
            current_quarter_month = qm
            break
    
    if current_quarter_month is None:
        current_quarter_month = 3
        current_quarter_year = base_year + 1
    
    current_quarter_contract = f"{current_quarter_year % 100:02d}{current_quarter_month:02d}"
    
    # 检查本季合约是否与本月或下月合约重复
    if current_quarter_contract in [current_month, next_month_contract]:
        # 如果重复，将本季和下季合约都往后推一个季度
        if current_quarter_month == 12:
            current_quarter_month = 3
            current_quarter_year += 1
        else:
            current_quarter_month = quarter_months[quarter_months.index(current_quarter_month) + 1]
        
        current_quarter_contract = f"{current_quarter_year % 100:02d}{current_quarter_month:02d}"
    
    contract_months.append(current_quarter_contract)
    
    # 下季合约
    if current_quarter_month == 12:
        next_quarter_month = 3
        next_quarter_year = current_quarter_year + 1
    else:
        next_quarter_month = quarter_months[quarter_months.index(current_quarter_month) + 1]
        next_quarter_year = current_quarter_year
    
    next_quarter_contract = f"{next_quarter_year % 100:02d}{next_quarter_month:02d}"
    contract_months.append(next_quarter_contract)
    
    return contract_months

# 显示当前使用的合约月份
current_contract_months = get_contract_months()
st.info(f"📅 当前使用的合约月份: {', '.join(current_contract_months)} (根据第4个星期三规则自动计算)")

# 顶部控制栏 - 包含保存按钮和刷新控制
col1, col2, col3 = st.columns([1.5, 2, 2.5])
with col1:
    save_button = st.button("💾 保存当前数据到GitHub", help="将当前数据保存到GitHub仓库")
with col2:
    refresh_button = st.button("🔄 手动刷新数据")
with col3:
    auto_refresh = st.checkbox("启用自动刷新(每5分钟)", value=True)

# 上次更新时间显示
last_update = st.empty()

# 获取上一个交易日的函数
def get_previous_trade_date():
    """获取上一个交易日的日期"""
    today = datetime.date.today()
    # 如果今天是周一，上一个交易日是上周五
    if today.weekday() == 0:  # 周一
        previous_date = today - datetime.timedelta(days=3)
    # 如果今天是周日，上一个交易日是上周五
    elif today.weekday() == 6:  # 周日
        previous_date = today - datetime.timedelta(days=2)
    # 其他情况，上一个交易日是昨天
    else:
        previous_date = today - datetime.timedelta(days=1)
    
    return previous_date.strftime("%Y%m%d")

# 建立期权代码映射关系
@st.cache_data(ttl=43200)  # 缓存12小时
def get_option_code_mapping():
    """建立CONTRACT_ID到SECURITY_ID的映射关系"""
    mapping = {}
    
    def get_previous_working_days(num_days=10):
        """获取上一个工作日开始的日期列表，排除周六周日"""
        dates = []
        current_date = datetime.date.today()
        
        while len(dates) < num_days:
            current_date -= datetime.timedelta(days=1)
            # 跳过周六(5)和周日(6)
            if current_date.weekday() < 5:  # 0-4是周一到周五
                dates.append(current_date.strftime("%Y%m%d"))
        
        return dates
    
    try:
        # 获取最近的工作日列表
        working_dates = get_previous_working_days(10)  # 获取最近10个工作日
        
        option_risk_df = None
        used_date = None
        
        # 尝试多个工作日期，找到一个有效的
        for date in working_dates:
            try:
                option_risk_df = ak.option_risk_indicator_sse(date=date)
                if not option_risk_df.empty:
                    used_date = date
                    break
            except Exception as date_error:
                continue
        
        if option_risk_df is None or option_risk_df.empty:
            return {}
        
        # 检查是否有期望的列名
        actual_columns = list(option_risk_df.columns)
        required_columns = ['SECURITY_ID', 'CONTRACT_ID', 'CONTRACT_SYMBOL']
        missing_columns = [col for col in required_columns if col not in actual_columns]
        
        if missing_columns:
            return {}
        
        # 建立CONTRACT_ID到SECURITY_ID的映射
        for _, row in option_risk_df.iterrows():
            try:
                contract_id = str(row['CONTRACT_ID'])
                security_id = str(row['SECURITY_ID'])
                contract_symbol = str(row['CONTRACT_SYMBOL'])
                
                # 建立映射关系
                mapping[contract_id] = {
                    'security_id': security_id,
                    'contract_symbol': contract_symbol
                }
                
            except Exception as row_error:
                continue
        
        return mapping
        
    except Exception as e:
        return {}

# 获取实时期权价格
def get_real_time_option_price(security_id, option_type):
    """获取实时期权价格，Call使用卖价，Put使用买价"""
    try:
        option_data = ak.option_sse_spot_price_sina(symbol=security_id)
        
        if option_type == 'C':  # Call期权使用卖价
            try:
                price = float(option_data[option_data['字段'] == '卖价']['值'].iloc[0])
                if price <= 0:  # 如果卖价为0或负数，使用最新价
                    price = float(option_data[option_data['字段'] == '最新价']['值'].iloc[0])
            except (IndexError, KeyError, ValueError):
                # 如果卖价不可用，尝试最新价
                price = float(option_data[option_data['字段'] == '最新价']['值'].iloc[0])
        else:  # Put期权使用买价
            try:
                price = float(option_data[option_data['字段'] == '买价']['值'].iloc[0])
                if price <= 0:  # 如果买价为0或负数，使用最新价
                    price = float(option_data[option_data['字段'] == '最新价']['值'].iloc[0])
            except (IndexError, KeyError, ValueError):
                # 如果买价不可用，尝试最新价
                price = float(option_data[option_data['字段'] == '最新价']['值'].iloc[0])
            
        return round(price, 4) if price > 0 else None  # 保留4位小数
        
    except Exception as e:
        return None

# 获取基础期权数据（缓存12小时）
@st.cache_data(ttl=43200)
def get_basic_option_data():
    """获取基础期权数据，缓存12小时"""
    etf_symbols = [
        "华泰柏瑞沪深300ETF期权",      # 300ETF
        "南方中证500ETF期权",          # 500ETF
        "华夏上证50ETF期权",           # 50ETF
        "华夏科创50ETF期权",           # 科创50ETF
        "易方达科创50ETF期权"          # 科创板50ETF
    ]
    
    # 自动获取合约月份
    contract_months = get_contract_months()
    
    all_option_data = []
    for symbol in etf_symbols:
        for month in contract_months:
            try:
                option_data = ak.option_finance_board(symbol=symbol, end_month=month)
                if not option_data.empty:
                    option_data['ETF类型'] = symbol
                    all_option_data.append(option_data)
            except Exception as e:
                st.warning(f"获取 {symbol} {month} 月合约失败: {str(e)}")
                continue
    
    if not all_option_data:
        return pd.DataFrame()
    
    option_finance_board_df = pd.concat(all_option_data)
    # 从合约交易代码中提取月份信息
    option_finance_board_df['合约月份'] = option_finance_board_df['合约交易代码'].str[7:11]
    
    return option_finance_board_df

# 获取实时ETF价格（不缓存，每次都获取最新价格）
def get_real_time_etf_prices():
    """获取实时ETF价格"""
    etf_config = {
        "sh510300": {"name": "300ETF", "keywords": ["沪深300", "300ETF"]},
        "sh510500": {"name": "500ETF", "keywords": ["中证500", "500ETF"]},
        "sh510050": {"name": "50ETF", "keywords": ["上证50", "50ETF"]},
        "sh588000": {"name": "科创50ETF", "keywords": ["华夏科创50", "科创50ETF"]},
        "sh588080": {"name": "科创板50ETF", "keywords": ["易方达科创50", "科创板50ETF", "易方达"]}
    }
    
    etf_prices = {}
    for symbol, config in etf_config.items():
        try:
            spot_price_df = ak.option_sse_underlying_spot_price_sina(symbol=symbol)
            current_price = float(spot_price_df.loc[spot_price_df['字段'] == '最近成交价', '值'].iloc[0])
            etf_prices[symbol] = round(current_price, 4)  # 保留4位小数
        except Exception as e:
            st.warning(f"获取 {config['name']} 价格失败: {str(e)}")
            etf_prices[symbol] = 0.0  # 设置默认值
    
    return etf_config, etf_prices

# 主数据获取和展示函数
def get_and_display_data():
    # 创建进度条
    progress_bar = st.progress(0)
    progress_text = st.empty()
    
    # 更新进度函数
    def update_progress(progress, text):
        progress_bar.progress(progress / 100)
        progress_text.text(f"🔄 {text} ({progress}%)")
    
    try:
        # 步骤1: 获取期权代码映射关系（缓存12小时）- 10%
        update_progress(5, "正在获取期权代码映射关系...")
        option_mapping = get_option_code_mapping()
        update_progress(10, "期权代码映射关系获取完成")
        
        # 步骤2: 获取基础期权数据（缓存12小时）- 30%
        update_progress(15, "正在获取基础期权数据...")
        option_finance_board_df = get_basic_option_data()
        update_progress(30, "基础期权数据获取完成")
        
        if option_finance_board_df.empty:
            st.error("未能获取任何有效的期权数据")
            update_progress(100, "数据获取失败")
            return
        
        # 步骤3: 获取实时ETF价格 - 50%
        update_progress(35, "正在获取实时ETF价格...")
        etf_config, etf_prices = get_real_time_etf_prices()
        update_progress(50, "实时ETF价格获取完成")
        
        # 显示ETF价格（多列布局）
        price_cols = st.columns(len(etf_config))
        for i, (symbol, config) in enumerate(etf_config.items()):
            with price_cols[i]:
                price = etf_prices.get(symbol, 0.0)
                if price > 0:
                    st.metric(f"{config['name']}价格", f"{price:.4f}")
                else:
                    st.metric(f"{config['name']}价格", "获取失败", delta="❌")
        
        # 统计实时价格获取情况
        real_time_count = {'call_success': 0, 'put_success': 0, 'call_total': 0, 'put_total': 0}
        
        # 改进的ETF类型识别函数
        def get_etf_price(etf_type_name):
            """根据ETF类型名称获取对应的ETF价格"""
            # 创建所有可能的匹配项，按关键词长度降序排列
            matches = []
            for symbol, config in etf_config.items():
                for keyword in config['keywords']:
                    if keyword in etf_type_name:
                        matches.append((len(keyword), symbol, keyword))
            
            # 按关键词长度降序排序，优先匹配更具体的关键词
            matches.sort(reverse=True)
            
            if matches:
                return etf_prices.get(matches[0][1], 0.0)
            
            # 默认返回300ETF价格
            return etf_prices.get("sh510300", 0.0)
        
        # 步骤4: 开始计算贴水 - 60%
        update_progress(55, "正在计算期权贴水...")
        
        # 计算贴水
        def calculate_premium(group):
            calls = group[group['合约交易代码'].str.contains('C')]
            puts = group[group['合约交易代码'].str.contains('P')]
            
            if len(calls) > 0 and len(puts) > 0:
                real_time_count['call_total'] += 1
                real_time_count['put_total'] += 1
                
                # 获取Call期权实时价格（使用卖价）
                call_contract_code = calls.iloc[0]['合约交易代码']
                call_strike = calls.iloc[0]['行权价']
                
                # 直接使用合约交易代码作为CONTRACT_ID在映射中查找
                call_security_id = None
                if call_contract_code in option_mapping:
                    call_security_id = option_mapping[call_contract_code]['security_id']
                
                # 获取Put期权实时价格（使用买价）
                put_contract_code = puts.iloc[0]['合约交易代码']
                put_strike = puts.iloc[0]['行权价']
                
                # 直接使用合约交易代码作为CONTRACT_ID在映射中查找
                put_security_id = None
                if put_contract_code in option_mapping:
                    put_security_id = option_mapping[put_contract_code]['security_id']
                
                # 获取实时价格
                if call_security_id:
                    call_price = get_real_time_option_price(call_security_id, 'C')
                    if call_price is not None:
                        real_time_count['call_success'] += 1
                else:
                    call_price = calls.iloc[0]['当前价']  # fallback到原有数据
                
                if put_security_id:
                    put_price = get_real_time_option_price(put_security_id, 'P')
                    if put_price is not None:
                        real_time_count['put_success'] += 1
                else:
                    put_price = puts.iloc[0]['当前价']  # fallback到原有数据
                
                # 如果无法获取实时价格，使用原有数据
                if call_price is None:
                    call_price = calls.iloc[0]['当前价']
                if put_price is None:
                    put_price = puts.iloc[0]['当前价']
                
                strike = calls.iloc[0]['行权价']
                
                # 使用改进的ETF价格获取函数
                etf_price = get_etf_price(group.name[0])
                if etf_price <= 0:
                    return None  # 如果ETF价格获取失败，跳过计算
                    
                synthetic_price = call_price - put_price + strike
                premium_value = synthetic_price - etf_price
                
                # 精确计算剩余天数（每月第4个星期三到期）
                expiry_date_str = calls.iloc[0]['合约交易代码'][7:11]  # 格式如"2506"
                year = 2000 + int(expiry_date_str[:2])  # 前两位是年份
                month = int(expiry_date_str[2:4])       # 后两位是月份
                first_day = datetime.date(year, month, 1)
                # 计算第一个星期三
                first_wednesday = first_day + datetime.timedelta(days=(2 - first_day.weekday()) % 7)
                # 第四个星期三 = 第一个星期三 + 3周
                expiry_date = first_wednesday + datetime.timedelta(weeks=3)
                days_to_maturity = (expiry_date - datetime.date.today()).days
                
                return pd.Series({
                    '贴水价值': round(premium_value, 4),
                    '年化贴水率': round((premium_value / etf_price) * (365 / max(days_to_maturity, 1)), 4),  # 避免除以0
                    '剩余天数': int(days_to_maturity)  # 只保留整数部分
                })
        
        # 计算贴水
        premium_df = option_finance_board_df.groupby(['ETF类型', '合约月份', '行权价']).apply(calculate_premium).reset_index()
        update_progress(80, "期权贴水计算完成")
        
        # 确保合约月份列存在后再进行后续操作
        if '合约月份' not in option_finance_board_df.columns:
            st.error("无法从合约交易代码中提取月份信息")
            update_progress(100, "数据处理失败")
            return
        
        # 步骤5: 数据处理和展示准备 - 90%
        update_progress(85, "正在处理数据...")
        
        # 移除空值行
        premium_df = premium_df.dropna()
        
        # 改进的ETF类型名称显示
        etf_display_names = {
            "华泰柏瑞沪深300ETF期权": "300ETF",
            "南方中证500ETF期权": "500ETF", 
            "华夏上证50ETF期权": "50ETF",
            "华夏科创50ETF期权": "科创50ETF",
            "易方达科创50ETF期权": "科创板50ETF"
        }
        
        # 步骤6: 显示数据 - 100%
        update_progress(95, "正在生成数据展示...")
        
        # 显示数据 - 使用更灵活的列布局
        if not premium_df.empty:
            # 计算需要的列数
            unique_combinations = premium_df.groupby(['ETF类型', '合约月份']).size()
            num_combinations = len(unique_combinations)
            
            # 动态调整列数，最多4列
            num_cols = min(4, num_combinations)
            cols = st.columns(num_cols)
            
            for i, ((etf_type, month), group) in enumerate(premium_df.groupby(['ETF类型', '合约月份'])):
                with cols[i % num_cols]:  # 循环使用列
                    # 替换ETF类型名称
                    display_name = etf_display_names.get(etf_type, etf_type)
                    st.subheader(f"{display_name} - {month}月合约")
                    
                    # 复制一份数据避免修改原始数据
                    display_df = group.copy()
                    # 将年化贴水率转换为百分比格式前先排序
                    display_df = display_df.sort_values('年化贴水率', ascending=True)
                    # 将年化贴水率转换为百分比格式，保留4位小数
                    display_df['年化贴水率'] = (display_df['年化贴水率'] * 100).round(4).astype(str) + '%'
                    # 对其他数值列进行4位小数格式化
                    if '贴水价值' in display_df.columns:
                        display_df['贴水价值'] = display_df['贴水价值'].round(4)
                    if '行权价' in display_df.columns:
                        display_df['行权价'] = display_df['行权价'].round(4)
                    if '剩余天数' in display_df.columns:
                        display_df['剩余天数'] = display_df['剩余天数'].astype(int)  # 只保留整数部分
                    # 设置紧凑布局
                    st.dataframe(
                        display_df[['行权价', '贴水价值', '年化贴水率', '剩余天数']],
                        use_container_width=True,
                        height=300,  # 调整高度适应更多数据
                        hide_index=True,  # 隐藏索引
                        column_config={
                            "行权价": st.column_config.NumberColumn(width="small", format="%.4f"),
                            "贴水价值": st.column_config.NumberColumn(width="small", format="%.4f"),
                            "年化贴水率": st.column_config.TextColumn(width="small"),
                            "剩余天数": st.column_config.NumberColumn(width="small", format="%d")  # 整数格式
                        }
                    )
        else:
            st.warning("未能计算出任何有效的贴水数据")
        
        # 完成
        update_progress(100, "数据刷新完成！")
        
        # 更新最后刷新时间
        beijing_tz = datetime.timezone(datetime.timedelta(hours=8))
        beijing_time = datetime.datetime.now(beijing_tz)
        last_update.text(f"最后更新时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')} (北京时间)")

        # 将结果存储到全局变量中
        st.session_state.latest_premium_data = premium_df
        
    except Exception as e:
        st.error(f"数据获取过程中出现错误: {str(e)}")
        update_progress(100, "数据获取失败")
    finally:
        # 延迟一下让用户看到100%完成状态
        time.sleep(0.5)
        progress_bar.empty()
        progress_text.empty()

# 处理保存按钮点击
if save_button:
    save_data_to_github()

# 主要的数据显示逻辑
# 初始化会话状态
if 'last_refresh_time' not in st.session_state:
    st.session_state.last_refresh_time = time.time()

# 检查是否需要刷新数据
current_time = time.time()
time_since_refresh = current_time - st.session_state.last_refresh_time

# 手动刷新按钮逻辑
if refresh_button:
    st.session_state.last_refresh_time = time.time()
    # 清除缓存以强制重新获取数据
    get_option_code_mapping.clear()
    get_basic_option_data.clear()
    st.rerun()  # 立即刷新

# 自动刷新检查 - 如果到时间就立即刷新
if auto_refresh and time_since_refresh >= 300:
    st.session_state.last_refresh_time = time.time()
    # 清除缓存以强制重新获取数据
    get_option_code_mapping.clear()
    get_basic_option_data.clear()
    st.rerun()  # 立即刷新

# 显示数据
get_and_display_data()

# 移除自动刷新后台检查，避免阻塞主线程
# 自动刷新将通过页面重新加载来实现，而不是后台sleep