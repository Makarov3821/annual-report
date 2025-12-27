import os
import time
import pickle
import argparse
import statistics
import re
import multiprocessing
import bisect
from functools import partial

# --- 核心辅助函数 ---
def timestamp_2_mytime(timestamp):
    return time.strftime('%Y,%m,%d,%H,%M,%S', time.localtime(timestamp))

def mytime_2_timestamp(mytime):
    return time.mktime(time.strptime(mytime, '%Y,%m,%d,%H,%M,%S'))

def check_timestamp_is_inside(start_time, end_time, target_time):
    return start_time <= target_time <= end_time

def extract_hms_from_timestamp(timestamp):
    return time.strftime('%H%M%S', time.localtime(timestamp))

def extract_md_from_timestamp(timestamp):
    return time.strftime('%m%d', time.localtime(timestamp))

# --- 正则修复 ---
# 旧正则: r'"([^"]*)"...' 遇到 "" 会失败
# 新正则: r'"((?:[^"]|"")*)"...' 能匹配包含 "" 的字段
CPU_TIME_PATTERN = re.compile(r'"((?:[^"]|"")*)"\s+"((?:[^"]|"")*)"\s+([0-9\.]+)')

def process_single_file(file_path, year, year_start, year_end):
    """ 单个文件处理函数 """
    local_data = []
    if not os.path.exists(file_path): return []
    
    print(f"🚀 [PID {os.getpid()}] Processing: {os.path.basename(file_path)}")
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                if "JOB_FINISH" not in line: continue
                try:
                    parts = line.split()
                    if len(parts) < 20: continue

                    user = parts[11].strip('"')
                    queue = parts[12].strip('"')
                    timesub_stamp = int(parts[7])
                    timestart_stamp = int(parts[10])
                    timeend_stamp = int(parts[2])
                    
                    try: cores = int(parts[23]) 
                    except: cores = 1

                    if timestart_stamp == 0: continue
                    if not check_timestamp_is_inside(year_start, year_end, timesub_stamp): continue

                    # --- 修复核心：智能提取 CPU Time ---
                    matches = CPU_TIME_PATTERN.findall(line)
                    cpu_time = 0.0
                    command_str = ""
                    
                    found_valid_cpu = False
                    for m in matches:
                        # m = (Group1_Str, Group2_Str, Group3_Num)
                        g1_str = m[0]
                        g2_str = m[1]
                        g3_num = m[2]

                        try:
                            val = float(g3_num)
                            
                            # 1. 过滤头部的时间戳 (Group2如果是纯数字字符串，通常是timestamp)
                            #    例如: "%J.err" "1733150264.13" 0
                            try:
                                if float(g2_str) > 0: 
                                    continue # Group2 是数字，说明这是 timestamp 字段，跳过
                            except:
                                pass # Group2 不是数字，可能是 Command，继续检查

                            # 2. 过滤 JOB_FINISH
                            if g1_str == "JOB_FINISH": continue
                            
                            # 3. 过滤 "default" (Ask String 字段)
                            if g2_str == "default": continue

                            # 4. 过滤 Host 列表 (通常 Host1 == Host2，或者是在 Host 列表末尾)
                            #    例如: "hostA" "hostA" 64 (Int)
                            #    而 Command 字段通常 G1(JobName) != G2(Command)
                            if g1_str == g2_str: continue
                            
                            # 5. 过滤空字符串
                            if g1_str == "" and g2_str == "": continue

                            # 通过所有过滤，认为是有效的 CPU Time
                            # 我们不断更新 cpu_time，因为 LSF 日志中 Command 字段出现在 Host 字段之后
                            # 最后的有效匹配通常就是 Command
                            command_str = g2_str
                            cpu_time = val
                            found_valid_cpu = True

                        except:
                            continue
                            
                    if not found_valid_cpu:
                        continue
                    # -------------------------------

                    # 软件识别
                    soft_mark = 0
                    line_soft = line.lower()   
                    software = "others"
                    if "g16" in line_soft or "g09" in line_soft or "g03" in line_soft and ".gjf" in line_soft and soft_mark == 0:
                        software = "gaussian"
                        soft_mark = 1
                    elif "vasp" in line_soft and "mpirun" in line_soft and soft_mark == 0:
                        software = "vasp"
                        soft_mark = 1
                    elif "qchem" in line_soft and soft_mark == 0:
                        software = "qchem"
                        soft_mark = 1
                    elif "cp2k" in line_soft and soft_mark == 0:
                        software = "cp2k"
                        soft_mark = 1
                    elif "lmp " in line_soft or "lmp_" in line_soft or "lmp-" in line_soft or "lammps" in line_soft or "LAMMPS" in line_soft and soft_mark == 0:
                        software = "lammps"
                        soft_mark = 1
                    elif "pmemd" in line_soft and soft_mark == 0:
                        software = "amber"
                        soft_mark = 1
                    elif "gmx " in line_soft and soft_mark == 0:
                        software = "gromacs"
                        soft_mark = 1
                    elif "namd2 " in line_soft or "namd3 " in line_soft or "charmrun" in line_soft and soft_mark == 0:
                        software = "namd"
                        soft_mark = 1
                    elif "xtb " in line_soft and soft_mark == 0:
                        software = "xtb"
                        soft_mark = 1
                    elif "orca" in line_soft and "openmpi" in line_soft and soft_mark == 0:
                        software = "orca"
                        soft_mark = 1
                    elif "nwchem " in line_soft and soft_mark == 0:
                        software = "nwchem"
                        soft_mark = 1
                    elif "rest" in line_soft and soft_mark == 0:
                        software = "rest"
                        soft_mark = 1
                    elif "xcfour" in line_soft and soft_mark == 0:
                        software = "cfour"
                        soft_mark = 1
                    elif "molcas" in line_soft or "pymolcas " in line_soft and soft_mark == 0:
                        software = "molcas"
                        soft_mark = 1
                    elif "molpro" in line_soft and soft_mark == 0:
                        software = "molpro"
                        soft_mark = 1
                    elif "psi4" in line_soft and soft_mark == 0:
                        software = "psi4"
                        soft_mark = 1
                    elif "pyscf" in line_soft and "python" in line_soft and soft_mark == 0:
                        software = "pyscf"
                        soft_mark = 1
                    elif "aims" in line_soft and soft_mark == 0:
                        software = "aims"
                        soft_mark = 1
                    elif "jdftx" in line_soft and soft_mark == 0:
                        software = "jdftx"
                        soft_mark = 1
                    elif "pw.x" in line_soft or "dos.x" in line_soft or "bands.x" in line_soft or "pp.x" in line_soft and soft_mark == 0:
                        software = "quantum espresso"
                        soft_mark = 1
                    elif "cmake" in line_soft and soft_mark == 0:
                        software = "cmake build"
                        soft_mark = 1
                    elif "make" in line_soft and soft_mark == 0:
                        software = "make build"
                        soft_mark = 1
                    elif "python" in line_soft or "python3" in line_soft and soft_mark == 0:
                        software = "python program"
                        soft_mark = 1
                    else:
                        software = "others"
                        soft_mark = 1
                    # --- 逻辑结束 ---
                    
                    run_time = timeend_stamp - timestart_stamp
                    wait_time = timestart_stamp - timesub_stamp
                    
                    # 宽松过滤，保留真实长作业
                    if run_time > 365 * 86400: continue
                    if wait_time > 365 * 86400: continue

                    local_data.append([user, queue, timesub_stamp, cores, software, wait_time, run_time, cpu_time])
                except: continue
    except Exception as e: print(f"Error: {e}")
    print(f"✅ [PID {os.getpid()}] Finished {os.path.basename(file_path)}: {len(local_data)} jobs")
    return local_data

def calculate_distribution(data_list):
    """
    计算数据的频次分布 (更新后的细致分桶)
    """
    # 边界定义 (秒)
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
    labels = [
        "<10s", "10~30s", "30s~1m", 
        "1m~10m", "10m~30m", "30m~1h", 
        "1h~4h", "4h~1d", "1d~3d", 
        "3d~7d", ">7d"
    ]
    counts = [0] * len(labels)
    
    for val in data_list:
        idx = bisect.bisect_right(boundaries, val)
        counts[idx] += 1
        
    return dict(zip(labels, counts))

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-d', '--dir', required=True)
    argparser.add_argument('-y', '--year', type=int, required=True)
    argparser.add_argument('-c', '--cores', default=8, type=int)
    args = argparser.parse_args()

    start_t = time.time()
    year_start = mytime_2_timestamp(f"{args.year},01,01,00,00,00")
    year_end = mytime_2_timestamp(f"{args.year},12,31,23,59,59")

    # 1. 读取假期数据 (修复点)
    holiday_set = set()
    if os.path.exists("holidays.txt"):
        with open("holidays.txt", 'r') as f:
            for line in f:
                parts = line.split()
                if len(parts) == 2 and parts[0] == str(args.year):
                    holiday_set.add(parts[1]) # 格式 MMDD
    else:
        print("Warning: holidays.txt not found. Holiday count will be 0.")

    log_files = []
    if os.path.exists(args.dir):
        log_files = [os.path.join(args.dir, f) for f in os.listdir(args.dir) if "lsb.acct" in f]

    # 智能核数
    real_cpu = os.cpu_count() or 1
    pool_size = min(args.cores, len(log_files), real_cpu)
    if pool_size < 1: pool_size = 1

    print(f"Processing {len(log_files)} files with {pool_size} processes...")
    with multiprocessing.Pool(pool_size) as pool:
        func = partial(process_single_file, year=args.year, year_start=year_start, year_end=year_end)
        results = pool.map(func, log_files)

    raw_data = [item for sublist in results for item in sublist]
    print(f"Total jobs: {len(raw_data)}. Analyzing...")

    # 初始化结构
    base_dict = {
        'jobs_count': 0, 'runtime_sum': 0, 'cpu_time_sum': 0,
        'date': {}, 'queue': {}, 'software': {},
        'latest_time': "000000", 'latest_time_date': "0101",
        'biggest_runtime': 0, 'biggest_wait_time': 0,
        'runtime': [], 'wait_time': [], 'efficiency': [],
        'holiday_count': 0,
        'time_period': {"1-6": 0, "7-12": 0, "13-18": 0, "19-24": 0},
        'dist_runtime': {}, 'dist_waittime': {} 
    }
    
    all_dict = {"all": pickle.loads(pickle.dumps(base_dict))}

    for job in raw_data:
        user, queue, sub_ts, cores, soft, wait, run, cpu = job
        date_md = extract_md_from_timestamp(sub_ts)
        sub_hms = int(extract_hms_from_timestamp(sub_ts))
        
        eff = (cpu / (run * cores)) * 100 if run > 0 and cores > 0 else 0
        if eff > 100: eff = 100

        if user not in all_dict: all_dict[user] = pickle.loads(pickle.dumps(base_dict))

        for target in [user, "all"]:
            d = all_dict[target]
            d['jobs_count'] += 1
            d['runtime_sum'] += run
            d['cpu_time_sum'] += cpu
            d['date'][date_md] = d['date'].get(date_md, 0) + 1
            d['queue'][queue] = d['queue'].get(queue, 0) + 1
            d['software'][soft] = d['software'].get(soft, 0) + 1
            d['runtime'].append(run)
            d['wait_time'].append(wait)
            d['efficiency'].append(eff)
            
            # 2. 统计假期内卷 (修复点)
            if date_md in holiday_set:
                d['holiday_count'] += 1

            if sub_hms < 60000: d['time_period']["1-6"] += 1
            elif sub_hms < 120000: d['time_period']["7-12"] += 1
            elif sub_hms < 180000: d['time_period']["13-18"] += 1
            else: d['time_period']["19-24"] += 1

            if run > d['biggest_runtime']: d['biggest_runtime'] = run
            if wait > d['biggest_wait_time']: d['biggest_wait_time'] = wait
            if sub_hms < 60000 and sub_hms > int(d['latest_time']):
                d['latest_time'] = str(sub_hms).zfill(6)
                d['latest_time_date'] = date_md

    for user in all_dict:
        d = all_dict[user]
        if d['jobs_count'] == 0: continue

        d['mean_runtime'] = int(statistics.mean(d['runtime']))
        d['median_runtime'] = int(statistics.median(d['runtime']))
        d['mean_waittime'] = int(statistics.mean(d['wait_time']))
        d['median_waittime'] = int(statistics.median(d['wait_time']))
        d['mean_efficiency'] = round(statistics.mean(d['efficiency']), 2) if d['efficiency'] else 0.0
        
        d['most_freq_date'] = max(d['date'], key=d['date'].get)

        # 计算分布
        d['dist_runtime'] = calculate_distribution(d['runtime'])
        d['dist_waittime'] = calculate_distribution(d['wait_time'])

        del d['runtime']
        del d['wait_time']
        del d['efficiency']

    with open(f"{args.year}.bin", 'wb') as f:
        pickle.dump(all_dict, f)
    print(f"Done. Saved {args.year}.bin")

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
