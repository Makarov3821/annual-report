#!/usr/bin/env python3
import os
import pickle
import argparse
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.align import Align
from rich.layout import Layout
from rich import box
from rich.text import Text

# 初始化 Rich Console
console = Console()

def format_duration(seconds):
    """将秒数转换为人类可读的时间"""
    if seconds is None: return "0s"
    seconds = float(seconds)
    if seconds >= 31536000: return f"{round(seconds / 31536000, 2)}年"
    if seconds >= 2592000: return f"{round(seconds / 2592000, 2)}月"
    if seconds >= 604800: return f"{round(seconds / 604800, 2)}周"
    if seconds >= 86400: return f"{round(seconds / 86400, 2)}天"
    if seconds >= 3600: return f"{round(seconds / 3600, 1)}小时"
    if seconds >= 60: return f"{round(seconds / 60, 1)}分"
    return f"{int(seconds)}秒"

def format_time_hms(hms_str):
    """格式化 HHMMSS 字符串"""
    if not hms_str or len(str(hms_str)) < 6: return hms_str
    s = str(hms_str).zfill(6)
    return f"{s[:2]}:{s[2:4]}:{s[4:]}"

def get_star_user_and_val(data, key):
    """
    获取某项指标最高的非all用户及其数值
    返回: (username, value)
    """
    max_val = None
    star_user = "None"
    
    for user in data:
        if user == "all": continue
        if key not in data[user]: continue
        
        val = data[user][key]
        
        if max_val is None:
            max_val = val
            star_user = user
            continue
            
        try:
            if val > max_val:
                max_val = val
                star_user = user
        except TypeError:
            try:
                if float(val) > float(max_val):
                    max_val = val
                    star_user = user
            except:
                continue

    return star_user, max_val

def find_outlier_users(data):
    """找出单次作业时长最长和排队最久的用户及其数值"""
    longest_job_user = "Unknown"
    longest_job_time = 0
    longest_wait_user = "Unknown"
    longest_wait_time = 0

    for user in data:
        if user == "all": continue
        
        if data[user].get('biggest_runtime', 0) > longest_job_time:
            longest_job_time = data[user]['biggest_runtime']
            longest_job_user = user
            
        if data[user].get('biggest_wait_time', 0) > longest_wait_time:
            longest_wait_time = data[user]['biggest_wait_time']
            longest_wait_user = user
            
    return (longest_job_user, longest_job_time), (longest_wait_user, longest_wait_time)

def main():
    argparser = argparse.ArgumentParser(description="LSF 年度报告 (定制优化版)", usage="%(prog)s <year>")
    argparser.add_argument("year", help="年份", type=int)
    args = argparser.parse_args()

    username = os.popen("whoami").read().strip()
    
    # 路径设置
    data_path = f"/share/Pub/ylzhao/annual-report/data/{args.year}.bin"
    # data_path = f"{args.year}.bin" # 本地测试用

    if not os.path.exists(data_path):
        console.print(f"[bold red]错误：[/bold red] 找不到 {args.year} 年的数据文件: {data_path}")
        os._exit(1)

    with open(data_path, "rb") as f:
        data = pickle.load(f)

    if username not in data:
        console.print(f"[bold red]错误：[/bold red] 数据集中没有找到用户 {username}。")
        os._exit(1)

    ud = data[username] # User Data
    ad = data["all"]    # All Data

    # ================= 0. 头部 =================
    console.print(Panel(
        Align.center(f"[bold magenta]✨ {args.year} HPC Cluster Annual Report ✨[/bold magenta]\nUser: {username}"),
        border_style="magenta",
        padding=(0, 2)
    ))

    # ================= 1. 核心指标 (4格布局) =================
    # 准备数据
    u_jobs = ud['jobs_count']
    a_jobs = ad['jobs_count']
    
    u_wall = format_duration(ud['runtime_sum'])
    a_wall = format_duration(ad['runtime_sum'])
    
    u_cpu = format_duration(ud['cpu_time_sum'])
    a_cpu = format_duration(ad['cpu_time_sum'])
    
    u_eff = ud.get('mean_efficiency', 0)
    a_eff = ad.get('mean_efficiency', 0)
    
    # 效率颜色
    eff_color = "green" if u_eff > 80 else ("yellow" if u_eff > 50 else "red")
    
    grid = Table.grid(expand=True, padding=(0, 1))
    grid.add_column(ratio=1)
    grid.add_column(ratio=1)
    grid.add_column(ratio=1)
    grid.add_column(ratio=1)

    # 构造四个Panel的内容
    # 格式：大号自己的数据 \n 小号全组/平均数据
    p1 = f"[bold cyan]{u_jobs:,}[/bold cyan]\n[dim]全组: {a_jobs:,}[/dim]"
    p2 = f"[bold green]{u_wall}[/bold green]\n[dim]全组: {a_wall}[/dim]"
    p3 = f"[bold yellow]{u_cpu}[/bold yellow]\n[dim]全组: {a_cpu}[/dim]"
    p4 = f"[bold {eff_color}]{u_eff}%[/bold {eff_color}]\n[dim]平均: {a_eff}%[/dim]"

    grid.add_row(
        Panel(p1, title="📦 作业量", border_style="cyan"),
        Panel(p2, title="⏱️ 运行时长", border_style="green"),
        Panel(p3, title="🔥 CPU核时", border_style="yellow"),
        Panel(p4, title="⚡ 核时效率", border_style="white")
    )
    console.print(grid)
    console.print("")

    # ================= 2. 统计表格 (Mean vs Median) =================
    # 要求：不换行，字体亮度差异不大，无斜体，无Tip
    stats_table = Table(title="📊 作业统计详情", box=box.SIMPLE_HEAD, show_header=True, expand=True)
    stats_table.add_column("统计指标", style="bold") # 去掉dim
    stats_table.add_column(f"你的数据 ({username})", justify="center")
    stats_table.add_column("全组数据 (Cluster)", justify="center")

    stats_table.add_row(
        "平均作业时长", 
        format_duration(ud['mean_runtime']), 
        format_duration(ad['mean_runtime'])
    )
    stats_table.add_row(
        "中位作业时长", 
        format_duration(ud['median_runtime']), 
        format_duration(ad['median_runtime'])
    )
    stats_table.add_row(
        "平均等待时长", 
        format_duration(ud['mean_waittime']), 
        format_duration(ad['mean_waittime'])
    )
    stats_table.add_row(
        "中位等待时长", 
        format_duration(ud['median_waittime']), 
        format_duration(ad['median_waittime'])
    )
    console.print(stats_table)
    console.print("")

    # ================= 3. 作业提交习惯 (User vs Cluster) =================
    console.print("[bold]🕒 作业提交时段分布 (你 vs 全组)[/bold]")
    
    # 归一化处理，让条形图更直观
    u_max = max(ud['time_period'].values()) if max(ud['time_period'].values()) > 0 else 1
    a_max = max(ad['time_period'].values()) if max(ad['time_period'].values()) > 0 else 1
    
    period_labels = {
        "1-6": "01:00-06:00 (深夜)",
        "7-12": "07:00-12:00 (上午)",
        "13-18": "13:00-18:00 (下午)",
        "19-24": "19:00-24:00 (晚间)"
    }

    t_dist = Table(box=None, show_header=True, expand=True, padding=(0,1))
    t_dist.add_column("时段", width=20, style="dim")
    t_dist.add_column(f"你的活跃度", ratio=1)
    t_dist.add_column("全组活跃度", ratio=1)

    for k, label in period_labels.items():
        u_val = ud['time_period'][k]
        a_val = ad['time_period'][k]
        
        # 绘制进度条
        u_bar_len = int((u_val / u_max) * 20)
        a_bar_len = int((a_val / a_max) * 20)
        
        u_bar = f"[blue]{'█' * u_bar_len}[/blue] {u_val}"
        a_bar = f"[white]{'█' * a_bar_len}[/white] {a_val}"
        
        t_dist.add_row(label, u_bar, a_bar)
    
    console.print(t_dist)
    console.print("")

    # ================= 4. 用户画像 (Enhanced) =================
    # 提取更多个人数据
    my_max_run = format_duration(ud.get('biggest_runtime', 0))
    my_max_wait = format_duration(ud.get('biggest_wait_time', 0))
    my_latest = format_time_hms(ud.get('latest_time', '000000'))
    my_latest_date = ud.get('latest_time_date', 'Unknown')
    my_holiday = ud.get('holiday_count', 0)
    
    most_soft = max(ud['software'], key=ud['software'].get) if ud['software'] else "None"
    most_queue = max(ud['queue'], key=ud['queue'].get) if ud['queue'] else "None"
    
    persona_text = (
        f"💻 [bold]常用软件[/bold]: [green]{most_soft}[/green]   "
        f"🏃 [bold]常用队列[/bold]: [yellow]{most_queue}[/yellow]\n"
        f"📅 [bold]卷王时刻[/bold]: {ud['most_freq_date']} (提交 {ud['date'].get(ud['most_freq_date'],0)} 个)\n"
        f"🦉 [bold]最晚提交[/bold]: {my_latest_date} 的 {my_latest}   "
        f"🏖️ [bold]假期内卷[/bold]: {my_holiday} 个作业\n"
        f"⏳ [bold]最久运行[/bold]: {my_max_run}   "
        f"🛑 [bold]最久排队[/bold]: {my_max_wait}"
    )
    
    console.print(Panel(
        persona_text,
        title="🔍 用户画像 (User Persona)",
        border_style="blue",
        expand=True
    ))

    # ================= 5. 荣耀榜 (Hall of Fame + Fun Facts) =================
    console.print("\n[bold magenta]🏆 年度 HPC 荣耀榜 (Hall of Fame)[/bold magenta]")
    
    # 动态计算数据
    (long_job_user, long_job_val), (long_wait_user, long_wait_val) = find_outlier_users(data)
    star_cpu_user, star_cpu_val = get_star_user_and_val(data, 'cpu_time_sum')
    star_jobs_user, star_jobs_val = get_star_user_and_val(data, 'jobs_count')
    star_late_user, star_late_val = get_star_user_and_val(data, 'latest_time')
    star_holiday_user, star_holiday_val = get_star_user_and_val(data, 'holiday_count')

    # 辅助格式化函数
    def fmt_winner(u, v, unit=""):
        if u == username:
            return f"[bold yellow]{u}[/bold yellow] ({v}{unit}) [bold red]就是你！[/bold red]"
        return f"[cyan]{u}[/cyan] ({v}{unit})"

    hof_table = Table(box=box.MINIMAL_DOUBLE_HEAD, show_header=True, expand=True)
    hof_table.add_column("奖项 (Title)", style="bold yellow")
    hof_table.add_column("得主 & 数据 (Winner & Data)")
    hof_table.add_column("备注 (Description)", style="dim")

    # 填充数据
    hof_table.add_row(
        "CPU 核时之星", 
        fmt_winner(star_cpu_user, format_duration(star_cpu_val)), 
        "使用了全组最多的计算资源"
    )
    hof_table.add_row(
        "作业数量之星", 
        fmt_winner(star_jobs_user, f"{star_jobs_val:,}"), 
        "提交了数量最多的任务"
    )
    hof_table.add_row(
        "年度熬夜之星", 
        fmt_winner(star_late_user, format_time_hms(star_late_val)), 
        "在深夜最晚时间还在提交作业"
    )
    hof_table.add_row(
        "假期内卷之星", 
        fmt_winner(star_holiday_user, star_holiday_val, "个"), 
        "在法定节假日提交作业最多"
    )
    # 将之前的 Fun Facts 并入这里
    hof_table.add_row(
        "年度耐力之王", 
        fmt_winner(long_job_user, format_duration(long_job_val)), 
        "拥有全组运行时间最长的一个作业"
    )
    hof_table.add_row(
        "年度苦等之王", 
        fmt_winner(long_wait_user, format_duration(long_wait_val)), 
        "拥有全组排队时间最长的一个作业"
    )

    console.print(hof_table)
    console.print(f"\n[dim]Generated by LSF Annual Report. See you in {args.year + 1}! 👋[/dim]")

if __name__ == "__main__":
    main()
