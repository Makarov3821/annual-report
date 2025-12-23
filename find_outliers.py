import os
import time
import argparse

# 阈值设置：超过多少天视为异常？
ABNORMAL_DAYS = 30
ABNORMAL_SECONDS = ABNORMAL_DAYS * 24 * 3600

def timestamp_2_mytime(timestamp):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-d', '--dir', help='Log directory', required=True)
    args = argparser.parse_args()

    print(f"🔍 正在寻找超过 {ABNORMAL_DAYS} 天的异常作业...")

    if not os.path.exists(args.dir):
        print("目录不存在")
        return

    files = [os.path.join(args.dir, f) for f in os.listdir(args.dir) if "lsb.acct" in f]
    
    for file_path in files:
        with open(file_path, 'r', errors='replace') as f:
            for line in f:
                if "JOB_FINISH" not in line:
                    continue
                
                try:
                    parts = line.split()
                    # 长度检查，防止索引越界
                    if len(parts) < 15: continue

                    # 提取关键信息
                    # Index 12: Queue (队列)
                    queue = parts[12].strip('"')
                    user = parts[11].strip('"')
                    job_id = parts[3] 
                    
                    end_time = int(parts[2])
                    submit_time = int(parts[7])
                    start_time = int(parts[10])
                    
                    if start_time == 0: continue

                    run_time = end_time - start_time
                    wait_time = start_time - submit_time
                    
                    # 1. 检查运行时间异常
                    if run_time > ABNORMAL_SECONDS:
                        print(f"⚠️ [运行异常] User: {user} | 队列: {queue} | JobID: {job_id}")
                        print(f"   运行时长: {run_time/86400:.2f} 天")
                        print(f"   开始时间: {timestamp_2_mytime(start_time)}")
                        print(f"   结束时间: {timestamp_2_mytime(end_time)}")
                        print(f"   日志文件: {os.path.basename(file_path)}\n")

                    # 2. 检查排队时间异常
                    if wait_time > ABNORMAL_SECONDS:
                        print(f"⚠️ [排队异常] User: {user} | 队列: {queue} | JobID: {job_id}")
                        print(f"   排队时长: {wait_time/86400:.2f} 天")
                        print(f"   提交时间: {timestamp_2_mytime(submit_time)}")
                        print(f"   开始时间: {timestamp_2_mytime(start_time)}")
                        print(f"   日志文件: {os.path.basename(file_path)}\n")

                except Exception:
                    continue

if __name__ == "__main__":
    main()
