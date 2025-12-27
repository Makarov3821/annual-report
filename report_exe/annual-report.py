#!/usr/bin/env python3
import os
import pickle
import argparse
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.align import Align
from rich import box
from rich.bar import Bar

console = Console()

def format_duration(seconds):
    if seconds is None: return "0s"
    seconds = float(seconds)
    if seconds >= 31536000: return f"{round(seconds/31536000, 2)}年"
    if seconds >= 2592000: return f"{round(seconds/2592000, 2)}月"
    if seconds >= 604800: return f"{round(seconds/604800, 2)}周"
    if seconds >= 86400: return f"{round(seconds/86400, 2)}天"
    if seconds >= 3600: return f"{round(seconds/3600, 1)}时"
    if seconds >= 60: return f"{round(seconds/60, 1)}分"
    return f"{int(seconds)}秒"

def format_time_hms(hms):
    if not hms or len(str(hms)) < 6: return hms
    s = str(hms).zfill(6)
    return f"{s[:2]}:{s[2:4]}:{s[4:]}"

def get_bin_index_for_value(value):
    """根据数值判断它落在哪个分布区间 (需与 run_fast 保持一致)"""
    # <10s, 10-30s, 30s-1m, 1m-10m, 10m-30m, 30m-1h, 1h-4h, 4h-1d, 1d-3d, 3d-7d, >7d
    boundaries = [
        10, 
        30, 
        60,       # 1m
        600,      # 10m
        1800,     # 30m
        3600,     # 1h
        14400,    # 4h
        86400,    # 1d
        259200,   # 3d
        604800    # 7d
    ]
    import bisect
    return bisect.bisect_right(boundaries, value)

def get_monthly_distribution(date_dict):
    """
    将每日数据 {'0101': 5, ...} 聚合为月度数据 {'01': 100, ...}
    """
    monthly_counts = {str(i).zfill(2): 0 for i in range(1, 13)}
    if not date_dict:
        return monthly_counts
        
    for date_str, count in date_dict.items():
        # date_str 格式为 "MMDD"，取前两位
        if len(date_str) == 4:
            month = date_str[:2]
            if month in monthly_counts:
                monthly_counts[month] += count
    return monthly_counts

def draw_monthly_chart(u_month_dist, c_month_dist):
    """
    绘制月度趋势对比图
    """
    # 获取最大值用于归一化
    u_max = max(u_month_dist.values()) if u_month_dist and max(u_month_dist.values()) > 0 else 1
    c_max = max(c_month_dist.values()) if c_month_dist and max(c_month_dist.values()) > 0 else 1
    
    table = Table(box=None, show_header=True, expand=True, padding=(0,1))
    table.add_column("月份 (Month)", width=12, style="dim")
    table.add_column("你的作业量 (Your Jobs)", ratio=1)
    table.add_column("全组作业量 (Cluster Jobs)", ratio=1)
    
    # 月份名称映射
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    
    for i in range(1, 13):
        m_key = str(i).zfill(2)
        m_name = f"{i}月 ({month_names[i-1]})"
        
        u_val = u_month_dist.get(m_key, 0)
        c_val = c_month_dist.get(m_key, 0)
        
        # 绘制条形
        # 这里用定长 20 字符来做条形图基准
        u_bar_len = int((u_val / u_max) * 25)
        c_bar_len = int((c_val / c_max) * 25)
        
        u_bar = f"[blue]{'█' * u_bar_len}[/blue] {u_val}"
        c_bar = f"[dim]{'█' * c_bar_len}[/dim] {c_val}"
        
        # 如果是 0，显示淡色
        if u_val == 0: u_bar = "[dim]-[/dim]"
        if c_val == 0: c_bar = "[dim]-[/dim]"

        table.add_row(m_name, u_bar, c_bar)
        
    return table

def draw_dual_metric_histogram(dist_dict, u_mean, u_med, c_mean, c_med, title):
    """
    绘制直方图并标记用户位置 (带数值)
    """
    max_count = max(dist_dict.values()) if dist_dict else 1
    total_count = sum(dist_dict.values()) if dist_dict else 1
    
    # 准备 4 个指标的信息
    # 格式: (值, 颜色, 标签简写, 完整标签)
    metrics = [
        (u_mean, "green", "U-Mean", "User Mean"),
        (u_med, "cyan", "U-Med", "User Median"),
        (c_mean, "yellow", "C-Mean", "Cluster Mean"),
        (c_med, "magenta", "C-Med", "Cluster Median")
    ]
    
    # 计算每个指标落在哪个 bin
    # bin_markers[bin_index] = ["标签(数值)", ...]
    bin_markers = {}
    
    for val, color, label_short, label_full in metrics:
        idx = get_bin_index_for_value(val)
        val_str = format_duration(val)
        # 使用带颜色的文本
        marker_text = f"[{color}]{label_short}({val_str})[/{color}]"
        
        if idx not in bin_markers:
            bin_markers[idx] = []
        bin_markers[idx].append(marker_text)

    # 开始绘图
    table = Table(title=title, box=None, show_header=False, expand=True, padding=(0,1))
    table.add_column("Interval", width=12, style="dim", justify="right")
    table.add_column("Bar", ratio=1)
    table.add_column("Count", width=8, justify="right")
    table.add_column("Marker", width=40, style="bold") # 再次加宽以容纳多个标记

    for i, (label, count) in enumerate(dist_dict.items()):
        bar_len = int((count / max_count) * 40)
        percentage = (count / total_count) * 100
        
        # 基础条形图颜色 (淡蓝色)
        bar_color = "blue"
        bar_str = f"[{bar_color}]{'█' * bar_len}[/{bar_color}]"
        
        if bar_len == 0 and count > 0:
            bar_str = f"[{bar_color}]|[/{bar_color}]"

        # 构建 Marker 字符串
        marker_str = ""
        if i in bin_markers:
            # 如果有多个标记在同一行，用 " & " 连接
            marker_str = "← " + " & ".join(bin_markers[i])

        table.add_row(label, bar_str, f"{percentage:.1f}%", marker_str)
        
    return table

def find_outlier_users(data):
    longest_job_user = "Unknown"; longest_job_time = 0
    longest_wait_user = "Unknown"; longest_wait_time = 0
    for user in data:
        if user == "all": continue
        if data[user].get('biggest_runtime', 0) > longest_job_time:
            longest_job_time = data[user]['biggest_runtime']
            longest_job_user = user
        if data[user].get('biggest_wait_time', 0) > longest_wait_time:
            longest_wait_time = data[user]['biggest_wait_time']
            longest_wait_user = user
    return (longest_job_user, longest_job_time), (longest_wait_user, longest_wait_time)

def get_star_user_and_val(data, key):
    max_val = None; star_user = "None"
    for user in data:
        if user == "all": continue
        val = data[user].get(key)
        if val is None: continue
        if max_val is None: max_val = val; star_user = user; continue
        try:
            if val > max_val: max_val = val; star_user = user
        except: continue
    return star_user, max_val

def main():
    argparser = argparse.ArgumentParser(description="你的年度报告")
    argparser.add_argument("year", type=int)
    args = argparser.parse_args()
    username = os.popen("whoami").read().strip()
    
    # 路径请根据实际情况修改
    data_path = f"/share/Pub/ylzhao/annual-report/data/{args.year}.bin"
    # data_path = f"{args.year}.bin"

    if not os.path.exists(data_path):
        console.print(f"[red]No data found for {args.year}[/red]"); os._exit(1)
    with open(data_path, "rb") as f: data = pickle.load(f)
    if username not in data: console.print(f"[red]User {username} not found[/red]"); os._exit(1)

    ud = data[username]; ad = data["all"]

    # 1. Header
    console.print(Panel(Align.center(f"[bold magenta]✨ {args.year} HPC Cluster Annual Report ✨[/bold magenta]\nUser: {username}"), border_style="magenta"))

    # 2. Key Metrics
    u_eff = ud.get('mean_efficiency', 0)
    eff_color = "green" if u_eff > 80 else ("yellow" if u_eff > 50 else "red")
    
    grid = Table.grid(expand=True, padding=(0, 1))
    for _ in range(4): grid.add_column(ratio=1)
    
    grid.add_row(
        Panel(f"[bold cyan]{ud['jobs_count']:,}[/bold cyan]\n[dim]All: {ad['jobs_count']:,}[/dim]", title="📦 作业量(Jobs Count)", border_style="cyan"),
        Panel(f"[bold green]{format_duration(ud['runtime_sum'])}[/bold green]\n[dim]All: {format_duration(ad['runtime_sum'])}[/dim]", title="⏱️ 运行时长(Walltime)", border_style="green"),
        Panel(f"[bold yellow]{format_duration(ud['cpu_time_sum'])}[/bold yellow]\n[dim]All: {format_duration(ad['cpu_time_sum'])}[/dim]", title="🔥 CPU核时(CPU Time)", border_style="yellow"),
        Panel(f"[bold {eff_color}]{u_eff}%[/bold {eff_color}]\n[dim]Avg: {ad.get('mean_efficiency',0)}%[/dim]", title="⚡ 核时效率(Efficiency)", border_style="white")
    )
    console.print(grid); console.print("")

    # --- 新增模块：3. 月度作业趋势 ---
    console.print("[bold]📅 月度作业趋势 (Monthly Activity)[/bold]")
    # 实时计算月度数据
    u_month_dist = get_monthly_distribution(ud.get('date', {}))
    c_month_dist = get_monthly_distribution(ad.get('date', {}))
    console.print(draw_monthly_chart(u_month_dist, c_month_dist))
    console.print("")
    # --------------------------------

    # 4. Distribution Charts (Dual Metrics)
    if 'dist_runtime' in ad:
        console.print(draw_dual_metric_histogram(
            ad['dist_runtime'], 
            ud['mean_runtime'], ud['median_runtime'], 
            ad['mean_runtime'], ad['median_runtime'], 
            "📊 作业运行时长分布 (Walltime)"
        ))
        console.print("")
        console.print(draw_dual_metric_histogram(
            ad['dist_waittime'], 
            ud['mean_waittime'], ud['median_waittime'], 
            ad['mean_waittime'], ad['median_waittime'], 
            "⏳ 作业排队时长分布 (Pending Time)"
        ))
        
        # 图例说明
        legend = "Legend: " \
                 "[green]用户平均值(User Mean)[/green] | [cyan]用户中位数(User Median)[/cyan] | " \
                 "[yellow]集群平均值(Cluster Mean)[/yellow] | [magenta]集群中位数(Cluster Median)[/magenta]"
        console.print(Align.center(legend))
    else:
        console.print("[yellow]⚠️ Warning: Old data format detected. Please re-run run_fast_v5.py[/yellow]")

    console.print("")

    # 5. Habits
    console.print("[bold]🕒 作业提交习惯[/bold]")
    period_labels = {"1-6":"01-06(夜)", "7-12":"07-12(晨)", "13-18":"13-18(午)", "19-24":"19-24(晚)"}
    u_max = max(ud['time_period'].values()) or 1
    a_max = max(ad['time_period'].values()) or 1
    t_habits = Table(box=None, show_header=True, expand=True, padding=(0,1))
    t_habits.add_column("时段", width=12, style="dim")
    t_habits.add_column("你的活跃度", ratio=1)
    t_habits.add_column("集群活跃度", ratio=1)
    
    for k, lbl in period_labels.items():
        uv = ud['time_period'][k]; av = ad['time_period'][k]
        u_bar = f"[blue]{'█'*int(uv/u_max*20)}[/blue] {uv}"
        a_bar = f"[dim]{'█'*int(av/a_max*20)}[/dim] {av}"
        t_habits.add_row(lbl, u_bar, a_bar)
    console.print(t_habits); console.print("")

    # 6. Persona
    most_soft = max(ud['software'], key=ud['software'].get) if ud['software'] else "None"
    most_queue = max(ud['queue'], key=ud['queue'].get) if ud['queue'] else "None"
    my_max_run = format_duration(ud.get('biggest_runtime', 0))
    my_max_wait = format_duration(ud.get('biggest_wait_time', 0))
    my_latest = format_time_hms(ud.get('latest_time', '000000'))
    my_holiday = ud.get('holiday_count', 0)

    console.print(Panel(
        f"💻 [bold]常用软件[/bold]: [green]{most_soft}[/green]   🏃 [bold]常用队列[/bold]: [yellow]{most_queue}[/yellow]\n"
        f"🦉 [bold]最晚提交[/bold]: {my_latest}   🏖️ [bold]假期内卷[/bold]: {my_holiday}\n"
        f"⏳ [bold]最久运行[/bold]: {my_max_run}   🛑 [bold]最久排队[/bold]: {my_max_wait}",
        title="🔍 用户画像", border_style="blue"
    ))

    # 7. Hall of Fame
    console.print("\n[bold magenta]🏆 荣耀榜 (Hall of Fame)[/bold magenta]")
    (lj_u, lj_v), (lw_u, lw_v) = find_outlier_users(data)
    sc_u, sc_v = get_star_user_and_val(data, 'cpu_time_sum')
    sj_u, sj_v = get_star_user_and_val(data, 'jobs_count')
    sl_u, sl_v = get_star_user_and_val(data, 'latest_time')
    sh_u, sh_v = get_star_user_and_val(data, 'holiday_count')

    def fw(u, v): return f"[bold yellow]{u}[/bold yellow] ({v}) [bold red]YOU![/bold red]" if u==username else f"[cyan]{u}[/cyan] ({v})"
    
    hof = Table(box=box.MINIMAL_DOUBLE_HEAD, show_header=True, expand=True)
    hof.add_column("奖项", style="bold yellow")
    hof.add_column("得主")
    hof.add_column("描述", style="dim")
    
    hof.add_row("CPU核时王", fw(sc_u, format_duration(sc_v)), "使用最多CPU核时")
    hof.add_row("作业数量王", fw(sj_u, f"{sj_v:,}"), "提交了最多作业")
    hof.add_row("熬夜冠军", fw(sl_u, format_time_hms(sl_v)), "提交时间最晚")
    hof.add_row("假期卷王", fw(sh_u, sh_v), "假期提交作业最多")
    hof.add_row("耐力之王", fw(lj_u, format_duration(lj_v)), "单个作业最长运行")
    hof.add_row("苦等之王", fw(lw_u, format_duration(lw_v)), "单个作业最长排队")
    
    console.print(hof)
    console.print(f"\n[dim]See you in {args.year + 1}! 👋[/dim]")

if __name__ == "__main__":
    main()
