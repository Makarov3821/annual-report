import os
import time
import pickle
import argparse
import statistics
import re
import multiprocessing
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

# --- 预编译正则 ---
# 匹配逻辑：找到最后两个被引号包裹的字符串（JobName和Command），紧接着是CPU时间
CPU_TIME_PATTERN = re.compile(r'"([^"]*)"\s+"([^"]*)"\s+([0-9\.]+)')

def process_single_file(file_path, year, year_start, year_end):
    """
    单个文件处理函数
    """
    local_data = []
    
    if not os.path.exists(file_path):
        return []

    # 获取当前进程ID，方便查看并行状态
    pid = os.getpid()
    print(f"🚀 [PID {pid}] Start processing: {os.path.basename(file_path)}")
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                if "JOB_FINISH" not in line:
                    continue

                try:
                    parts = line.split()
                    if len(parts) < 20: continue

                    user = parts[11].strip('"')
                    queue = parts[12].strip('"')
                    timesub_stamp = int(parts[7])
                    timestart_stamp = int(parts[10])
                    timeend_stamp = int(parts[2])
                    
                    # 尝试提取核心数，默认位置23
                    try:
                        cores = int(parts[23]) 
                    except (IndexError, ValueError):
                        cores = 1 # 兜底

                    if timestart_stamp == 0: continue
                    
                    if not check_timestamp_is_inside(year_start, year_end, timesub_stamp):
                        continue

                    # 正则提取 Command 和 CPU Time
                    match = CPU_TIME_PATTERN.search(line)
                    if match:
                        command_str = match.group(2) # 保持原始大小写，后面再 lower()
                        cpu_time = float(match.group(3))
                    else:
                        continue

                    # --- 用户自定义软件识别逻辑 (完全恢复) ---
                    soft_mark = 0
                    line_soft = line.lower()   
                    software = "others" # 默认值，防止没匹配上

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

                    if run_time > 365 * 24 * 3600: continue 
                    if wait_time > 365 * 24 * 3600: continue
                    
                    local_data.append([user, queue, timesub_stamp, cores, software, wait_time, run_time, cpu_time])
                
                except (IndexError, ValueError):
                    continue
                    
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")

    print(f"✅ [PID {pid}] Finished {os.path.basename(file_path)}: {len(local_data)} jobs")
    return local_data

def main():
    argparser = argparse.ArgumentParser(description='Cluster Job Analyzer (Fast + Safe)')
    argparser.add_argument('-d', '--dir', help='Log directory', required=True, dest='dir')
    argparser.add_argument('-y', '--year', help='Year to analyse', required=True, dest='year', type=int)
    # 新增参数：限制使用的最大核心数
    argparser.add_argument('-c', '--cores', help='Max CPU cores to use (Default: 8)', default=8, type=int)
    args = argparser.parse_args()

    start_time_counter = time.time()

    year_start = mytime_2_timestamp(str(args.year) + ",01,01,00,00,00")
    year_end = mytime_2_timestamp(str(args.year) + ",12,31,23,59,59")

    log_files = []
    if os.path.exists(args.dir):
        for f in os.listdir(args.dir):
            if "lsb.acct" in f:
                log_files.append(os.path.join(args.dir, f))
    
    # --- 智能计算并发数 ---
    # 1. 用户设定的限制 (args.cores)
    # 2. 实际文件数量 (log_files) - 文件少就不需要开那么多进程
    # 3. 机器物理核心数 (os.cpu_count()) - 防止用户设定 9999
    real_cpu_count = os.cpu_count() or 1
    safe_cores = min(args.cores, len(log_files), real_cpu_count)
    
    # 如果 args.cores 强制指定很大，但文件也很多，为了安全我们还是尊重 args.cores，
    # 但建议不要超过物理核。这里逻辑是：只要不超过文件数（没必要）即可。
    # 最终逻辑：取 (用户限制) 和 (文件数) 的较小值。
    # 注意：如果 args.cores 设得比物理核还大，python 会自动调度，虽然慢但不会崩，
    # 但通常我们希望受限于 args.cores。
    pool_size = min(args.cores, len(log_files))
    if pool_size < 1: pool_size = 1

    print(f"Found {len(log_files)} lsb.acct files.")
    print(f"Starting multiprocessing pool with {pool_size} processes (User limit: {args.cores})...")

    with multiprocessing.Pool(processes=pool_size) as pool:
        func = partial(process_single_file, year=args.year, year_start=year_start, year_end=year_end)
        results_list = pool.map(func, log_files)

    raw_data = []
    for res in results_list:
        raw_data.extend(res)

    print(f"Data processing finished in {time.time() - start_time_counter:.2f} seconds.")
    print(f"Total valid jobs: {len(raw_data)}. Calculating statistics...")

    # --- 统计部分 (保持不变) ---
    holiday_date = []
    if os.path.exists("holidays.txt"):
        with open("holidays.txt", 'r') as f:
            holiday_raw = f.readlines()
        for line in holiday_raw:
            parts = line.split()
            if len(parts) == 2 and parts[0] == str(args.year):
                holiday_date.append(parts[1])

    base_user_dict = {
        'jobs_count': 0, 'runtime_sum': 0, 'cpu_time_sum': 0,
        'date': {}, 'queue': {}, 'software': {},
        'latest_time': "000000", 'latest_time_date': "0101",
        'biggest_runtime': 0, 'biggest_cpu_time': 0, 'biggest_wait_time': 0,
        'runtime': [], 'wait_time': [], 'efficiency': [],
        'holiday_count': 0,
        'time_period': {"1-6": 0, "7-12": 0, "13-18": 0, "19-24": 0}
    }
    
    all_dict = {"all": pickle.loads(pickle.dumps(base_user_dict))}

    for job in raw_data:
        user = job[0]
        queue = job[1]
        sub_time_hms = int(extract_hms_from_timestamp(job[2]))
        date_md = extract_md_from_timestamp(job[2])
        cores = job[3]
        software = job[4]
        wait_time = job[5]
        run_time = job[6]
        cpu_time = job[7]
        
        eff = 0
        if run_time > 0 and cores > 0:
            eff = (cpu_time / (run_time * cores)) * 100 
            if eff > 100: eff = 100

        if user not in all_dict:
            all_dict[user] = pickle.loads(pickle.dumps(base_user_dict))

        for target in [user, "all"]:
            d = all_dict[target]
            d['jobs_count'] += 1
            d['runtime_sum'] += run_time
            d['cpu_time_sum'] += cpu_time
            d['date'][date_md] = d['date'].get(date_md, 0) + 1
            d['queue'][queue] = d['queue'].get(queue, 0) + 1
            d['software'][software] = d['software'].get(software, 0) + 1
            d['runtime'].append(run_time)
            d['wait_time'].append(wait_time)
            d['efficiency'].append(eff)

            if 0 <= sub_time_hms < 60000: d['time_period']["1-6"] += 1
            elif sub_time_hms < 120000: d['time_period']["7-12"] += 1
            elif sub_time_hms < 180000: d['time_period']["13-18"] += 1
            else: d['time_period']["19-24"] += 1
            
            if run_time > d['biggest_runtime']: d['biggest_runtime'] = run_time
            if cpu_time > d['biggest_cpu_time']: d['biggest_cpu_time'] = cpu_time
            if wait_time > d['biggest_wait_time']: d['biggest_wait_time'] = wait_time
            
            if sub_time_hms < 60000 and sub_time_hms > int(d['latest_time']):
                d['latest_time'] = str(sub_time_hms).zfill(6)
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
        d['least_freq_date'] = min(d['date'], key=d['date'].get)
        
        if user != "all":
            for date_key, count in d['date'].items():
                if date_key in holiday_date:
                    d['holiday_count'] += count

        del d['runtime']
        del d['wait_time']
        del d['efficiency']

    filename = str(args.year) + ".bin"
    with open(filename, 'wb') as f:
        pickle.dump(all_dict, f)
    print(f"Done. Saved to {filename}")

if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
