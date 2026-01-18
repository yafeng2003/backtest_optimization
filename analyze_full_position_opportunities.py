"""
分析满仓日的股票机会
比较实际买入的股票盈亏 vs 未买入股票的潜在收益
"""
import pandas as pd
import os
from indicator_caculator import calculate_indicators
from strategies.strategy_marsi import StrategyMarsi
from modules.signals.buy_signal_1 import BuySignal1
from modules.signals.sell_signal_1 import SellSignal1
from modules.signals.buy_signal_2 import BuySignal2
from modules.signals.sell_signal_2 import SellSignal2
from modules.signals.buy_signal_3 import BuySignal3
from datetime import datetime, timedelta

# 初始化策略（用于判断卖出时机）
strategy = StrategyMarsi(
    buy_signal1=BuySignal1(),
    sell_signal1=SellSignal1(trailing_stop_rate=0.08),
    buy_signal2=BuySignal2(),
    sell_signal2=SellSignal2(),
    buy_signal3=BuySignal3(lookback_days=15, max_below_sma_ratio=0.8)
)

# 读取数据
full_position_df = pd.read_csv("backtest_results/StrategyMarsi/StrategyMarsi_full_position_candidates.csv")
trade_analysis_df = pd.read_csv("backtest_results/StrategyMarsi/StrategyMarsi_trade_analysis.csv")

print(f"满仓候选股票总数: {len(full_position_df)}")
print(f"实际买入: {len(full_position_df[full_position_df['actually_bought'] == True])}")
print(f"未能买入: {len(full_position_df[full_position_df['actually_bought'] == False])}")
print(f"交易记录总数: {len(trade_analysis_df)}")

# 1. 分析实际买入股票的盈亏情况
actually_bought_df = full_position_df[full_position_df['actually_bought'] == True].copy()

# 合并交易记录，找到对应的交易结果
# 需要匹配 stockno 和 entry_date（满仓日期的下一个交易日）
actually_bought_df['expected_entry_date'] = pd.to_datetime(actually_bought_df['date']) + pd.Timedelta(days=1)
trade_analysis_df['entry_date'] = pd.to_datetime(trade_analysis_df['entry_date'])
actually_bought_df['stockno'] = actually_bought_df['stockno'].astype(str).str.zfill(5)
trade_analysis_df['stockno'] = trade_analysis_df['stockno'].astype(str).str.zfill(5)

# 合并数据
bought_with_result = pd.merge(
    actually_bought_df,
    trade_analysis_df,
    on='stockno',
    how='left'
)

# 筛选出日期接近的交易（容忍3天内的差异，因为可能有停牌等情况）
bought_with_result['date_diff'] = abs(
    (bought_with_result['entry_date'] - bought_with_result['expected_entry_date']).dt.days
)
bought_with_result = bought_with_result[bought_with_result['date_diff'] <= 5]

print(f"\n成功匹配到交易记录的买入股票数: {len(bought_with_result)}")

# 2. 模拟未买入股票的潜在收益
not_bought_df = full_position_df[full_position_df['actually_bought'] == False].copy()
not_bought_df['stockno'] = not_bought_df['stockno'].astype(str).str.zfill(5)

print(f"\n开始模拟 {len(not_bought_df)} 只未买入股票的潜在收益...")

simulated_results = []

for idx, row in not_bought_df.iterrows():
    if idx % 100 == 0:
        print(f"进度: {idx}/{len(not_bought_df)}")
    
    stockno = row['stockno']
    buy_signal_date = row['date']
    
    # 读取股价数据
    data_file = f"data/{stockno}.HK_1d.csv"
    if not os.path.exists(data_file):
        continue
    
    try:
        price_df = pd.read_csv(data_file)
        # 标准化列名
        price_df.columns = price_df.columns.str.lower()
        if 'datetime' in price_df.columns:
            price_df.rename(columns={'datetime': 'date'}, inplace=True)
        price_df['date'] = pd.to_datetime(price_df['date'])
        price_df = price_df.sort_values('date')
        
        # 找到买入信号后的第一个交易日
        buy_signal_date_dt = pd.to_datetime(buy_signal_date)
        future_prices = price_df[price_df['date'] > buy_signal_date_dt]
        
        if len(future_prices) == 0:
            continue
        
        # 模拟买入
        entry_date = future_prices.iloc[0]['date']
        entry_price = future_prices.iloc[0]['open']
        
        # 模拟持有，使用追踪止损策略（与SellSignal1相同：回撤8%卖出）
        trailing_stop_rate = 0.08
        max_price = entry_price
        max_date = entry_date
        min_price = entry_price
        min_date = entry_date
        exit_price = None
        exit_date = None
        exit_type = "Not_Sold"
        
        # 遍历后续交易日，检查止损条件
        for i in range(1, min(len(future_prices), 200)):  # 最多持有200天
            current_date = future_prices.iloc[i]['date']
            current_price = future_prices.iloc[i]['close']
            
            # 更新最高价
            if current_price > max_price:
                max_price = current_price
                max_date = current_date
            
            # 更新最低价
            if current_price < min_price:
                min_price = current_price
                min_date = current_date
            
            # 检查追踪止损条件：从最高价回撤超过8%
            drawdown = (max_price - current_price) / max_price
            if drawdown >= trailing_stop_rate:
                # 触发止损，下一个交易日开盘价卖出
                if i + 1 < len(future_prices):
                    exit_date = future_prices.iloc[i + 1]['date']
                    exit_price = future_prices.iloc[i + 1]['open']
                else:
                    # 如果是最后一天，用当天收盘价
                    exit_date = current_date
                    exit_price = current_price
                exit_type = "Trailing_Stop"
                break
        
        # 如果没有卖出信号，使用最后一个价格
        if exit_price is None:
            exit_date = future_prices.iloc[-1]['date']
            exit_price = future_prices.iloc[-1]['close']
            exit_type = "End_of_Data"
        
        # 计算收益
        profit_rate = (exit_price - entry_price) / entry_price
        hold_days = (exit_date - entry_date).days
        max_float_rate = (max_price - entry_price) / entry_price
        min_float_rate = (min_price - entry_price) / entry_price
        
        simulated_results.append({
            'date': buy_signal_date,
            'stockno': stockno,
            'score': row['score'],
            'rank': row['rank'],
            'entry_date': entry_date.strftime('%Y-%m-%d'),
            'entry_price': entry_price,
            'exit_date': exit_date.strftime('%Y-%m-%d'),
            'exit_price': exit_price,
            'hold_days': hold_days,
            'profit_rate': profit_rate,
            'max_float_rate': max_float_rate,
            'max_float_date': max_date.strftime('%Y-%m-%d'),
            'min_float_rate': min_float_rate,
            'min_float_date': min_date.strftime('%Y-%m-%d'),
            'exit_type': exit_type
        })
        
    except Exception as e:
        print(f"处理 {stockno} 时出错: {e}")
        continue

print(f"\n成功模拟 {len(simulated_results)} 只未买入股票的潜在收益")

# 3. 生成对比报告
simulated_df = pd.DataFrame(simulated_results)

# 保存模拟结果
output_dir = "backtest_results/StrategyMarsi"
os.makedirs(output_dir, exist_ok=True)

simulated_df.to_csv(f"{output_dir}/missed_opportunities_simulation.csv", index=False, encoding='utf-8-sig')
print(f"\n未买入股票模拟结果已保存到: {output_dir}/missed_opportunities_simulation.csv")

# 4. 生成对比分析
print("\n" + "="*80)
print("对比分析：实际买入 vs 错过的机会")
print("="*80)

# 实际买入的统计
if len(bought_with_result) > 0:
    actual_profit = bought_with_result[bought_with_result['profit_rate'] > 0]
    actual_loss = bought_with_result[bought_with_result['profit_rate'] <= 0]
    
    print(f"\n【实际买入的股票】（满仓日买入）")
    print(f"总数: {len(bought_with_result)}")
    print(f"盈利: {len(actual_profit)} ({len(actual_profit)/len(bought_with_result)*100:.1f}%)")
    print(f"亏损: {len(actual_loss)} ({len(actual_loss)/len(bought_with_result)*100:.1f}%)")
    print(f"平均收益率: {bought_with_result['profit_rate'].mean()*100:.2f}%")
    print(f"平均持仓天数: {bought_with_result['hold_days'].mean():.1f}天")
    print(f"最大盈利: {bought_with_result['profit_rate'].max()*100:.2f}%")
    print(f"最大亏损: {bought_with_result['profit_rate'].min()*100:.2f}%")

# 未买入的统计
if len(simulated_df) > 0:
    missed_profit = simulated_df[simulated_df['profit_rate'] > 0]
    missed_loss = simulated_df[simulated_df['profit_rate'] <= 0]
    
    print(f"\n【未能买入的股票】（如果买入的模拟结果）")
    print(f"成功模拟: {len(simulated_df)}")
    print(f"盈利: {len(missed_profit)} ({len(missed_profit)/len(simulated_df)*100:.1f}%)")
    print(f"亏损: {len(missed_loss)} ({len(missed_loss)/len(simulated_df)*100:.1f}%)")
    print(f"平均收益率: {simulated_df['profit_rate'].mean()*100:.2f}%")
    print(f"平均持仓天数: {simulated_df['hold_days'].mean():.1f}天")
    print(f"最大盈利: {simulated_df['profit_rate'].max()*100:.2f}%")
    print(f"最大亏损: {simulated_df['profit_rate'].min()*100:.2f}%")

# 5. 找出最大的错过机会
if len(simulated_df) > 0:
    print(f"\n【TOP 10 错过的最佳机会】")
    top_missed = simulated_df.nlargest(10, 'profit_rate')
    print(top_missed[['date', 'stockno', 'rank', 'profit_rate', 'hold_days']].to_string(index=False))

# 6. 对比同一天的选择
if len(bought_with_result) > 0 and len(simulated_df) > 0:
    print(f"\n【同一满仓日的对比】")
    
    # 合并数据按日期分组
    bought_with_result['date'] = pd.to_datetime(bought_with_result['date'])
    simulated_df['date'] = pd.to_datetime(simulated_df['date'])
    
    # 找出有对比数据的日期
    common_dates = set(bought_with_result['date'].unique()) & set(simulated_df['date'].unique())
    
    comparison_results = []
    for date in sorted(common_dates):
        bought_on_date = bought_with_result[bought_with_result['date'] == date]
        missed_on_date = simulated_df[simulated_df['date'] == date]
        
        comparison_results.append({
            'date': date.strftime('%Y-%m-%d'),
            'bought_count': len(bought_on_date),
            'bought_avg_return': bought_on_date['profit_rate'].mean() * 100,
            'missed_count': len(missed_on_date),
            'missed_avg_return': missed_on_date['profit_rate'].mean() * 100,
            'return_diff': (missed_on_date['profit_rate'].mean() - bought_on_date['profit_rate'].mean()) * 100
        })
    
    comparison_df = pd.DataFrame(comparison_results)
    comparison_df.to_csv(f"{output_dir}/daily_comparison.csv", index=False, encoding='utf-8-sig')
    print(f"\n每日对比结果已保存到: {output_dir}/daily_comparison.csv")
    
    # 显示前10个差异最大的日期
    print(f"\n【前10个错过收益最大的日期】")
    top_diff = comparison_df.nlargest(10, 'return_diff')
    print(top_diff.to_string(index=False))

print("\n" + "="*80)
print("分析完成！")
print("="*80)
