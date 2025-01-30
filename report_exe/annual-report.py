#!/usr/bin/env python3
import os
import heapq
import pickle
import argparse

def main():
    argparser = argparse.ArgumentParser(description="你的年度报告。",usage="%(prog)s <year>")
    argparser.add_argument("year", help="年份", type=int)
    args = argparser.parse_args()

    #get username using popen
    username = os.popen("whoami").read().strip()

    #check if the data file exists
    if not os.path.exists("/share/Pub/ylzhao/annual-report/data/" + str(args.year) + ".bin"):
        print("似乎没有" + str(args.year) + "年的数据。")
        os._exit(1)

    #load data
    with open("/share/Pub/ylzhao/annual-report/data/" + str(args.year) + ".bin", "rb") as f:
        data = pickle.load(f)
    #check if the user exists
    if username not in data:
        print("似乎没有您的数据。")
        os._exit(1)
    
    user_jobs_count = data[username]["jobs_count"]
    user_runtime_sum = data[username]["runtime_sum"]
    #convert runtime to year or month or day
    if user_runtime_sum >= 31536000:
        user_runtime_sum_short = str(round(user_runtime_sum / 31536000, 2)) + "年"
    elif user_runtime_sum >= 2592000:
        user_runtime_sum_short = str(round(user_runtime_sum / 2592000, 2)) + "月"
    elif user_runtime_sum >= 86400:
        user_runtime_sum_short = str(round(user_runtime_sum / 86400, 2)) + "天"
    else:
        user_runtime_sum_short = False
    user_cpu_time_sum = data[username]["cpu_time_sum"]
    #convert cpu time to year or month or day
    if user_cpu_time_sum >= 31536000:
        user_cpu_time_sum_short = str(round(user_cpu_time_sum / 31536000, 2)) + "年"
    elif user_cpu_time_sum >= 2592000:
        user_cpu_time_sum_short = str(round(user_cpu_time_sum / 2592000, 2)) + "月"
    elif user_cpu_time_sum >= 86400:
        user_cpu_time_sum_short = str(round(user_cpu_time_sum / 86400, 2)) + "天"
    else:
        user_cpu_time_sum_short = False
    user_most_date = data[username]["most_freq_date"]
    user_most_date_num = data[username]["date"][user_most_date]
    user_least_date = data[username]["least_freq_date"]
    user_least_date_num = data[username]["date"][user_least_date]
    user_latest_time = data[username]["latest_time"]
    user_latest_time_date = data[username]["latest_time_date"]
    user_most_period = max(data[username]["time_period"], key=data[username]["time_period"].get)
    user_least_period = min(data[username]["time_period"], key=data[username]["time_period"].get)
    user_max_runtime_job = data[username]["biggest_runtime"]
    user_max_cpu_time_job = data[username]["biggest_cpu_time"]
    user_max_wait_time = data[username]["biggest_wait_time"]
    user_holiday_count = data[username]["holiday_count"]
    user_most_queue = max(data[username]["queue"], key=data[username]["queue"].get)
    user_most_software = max(data[username]["software"], key=data[username]["software"].get)
    user_mean_runtime = data[username]["mean_runtime"]
    user_median_runtime = data[username]["median_runtime"]
    user_mean_waittime = data[username]["mean_waittime"]
    user_median_waittime = data[username]["median_waittime"]


    all_jobs_count = data["all"]["jobs_count"]
    all_runtime_sum = data["all"]["runtime_sum"]
    all_cpu_time_sum = data["all"]["cpu_time_sum"]
    tmp_runtime = 0
    for user in data:
        if user != "all":
            if data[user]["runtime_sum"] > tmp_runtime:
                tmp_runtime = data[user]["runtime_sum"]
                all_most_runtime_user = user
    tmp_cpu_time = 0
    for user in data:
        if user != "all":
            if data[user]["cpu_time_sum"] > tmp_cpu_time:
                tmp_cpu_time = data[user]["cpu_time_sum"]
                all_most_cpu_time_user = user
    tmp_jobs_count = 0
    for user in data:
        if user != "all":
            if data[user]["jobs_count"] > tmp_jobs_count:
                tmp_jobs_count = data[user]["jobs_count"]
                all_most_jobs_count_user = user
    for user in data:
        if data[user]["latest_time"] == data["all"]["latest_time"]:
            all_latest_time_user = user
    all_latest_time = data["all"]["latest_time"]
    all_latest_time_user_date = data[all_latest_time_user]["latest_time_date"]
    #collect top three software
    all_top_three_software = heapq.nlargest(3, data["all"]["software"], key=data["all"]["software"].get)
    #collect top three queue
    all_top_three_queue = heapq.nlargest(3, data["all"]["queue"], key=data["all"]["queue"].get)
    all_most_period = max(data["all"]["time_period"], key=data["all"]["time_period"].get)
    all_mean_runtime = data["all"]["mean_runtime"]
    all_median_runtime = data["all"]["median_runtime"]
    all_mean_waittime = data["all"]["mean_waittime"]
    all_median_waittime = data["all"]["median_waittime"]


    #print the report
    print("嗨， " + username + "，这是你的" + str(args.year) + "年度报告：")
    if user_runtime_sum_short:
        print("你在" + str(args.year) + "年总共提交了" + str(user_jobs_count) + "个任务，作业运行时长总计" + str(user_runtime_sum) + "秒，" \
            "相当于" + user_runtime_sum_short + "；" \
            "CPU核时总计" + str(user_cpu_time_sum) + "秒，相当于" + user_cpu_time_sum_short + "。")
    else:
        print("你在" + str(args.year) + "年总共提交了" + str(user_jobs_count) + "个任务，作业运行时长总计" + str(user_runtime_sum) + "秒，" \
            "CPU核时总计" + str(user_cpu_time_sum) + "秒。")
    if username == all_most_runtime_user:
        print("你是组里的作业运行时长之星！")
    if username == all_most_cpu_time_user:
        print("你是组里的CPU核时之星！")
    if username == all_most_jobs_count_user:
        print("你是组里的提交任务数量之星！")
    print("你的平均作业时长是" + str(user_mean_runtime) + "秒，中位数是" + str(user_median_runtime) + "秒。")
    print("你的平均等待时长是" + str(user_mean_waittime) + "秒，中位数是" + str(user_median_waittime) + "秒。")
    if user_most_period == all_most_period:
        print("你最常在" + user_most_period + "时段提交作业。恰巧大家也都最常在这个时段交作业，真是太和群啦。")
    else:
        print("你最常在" + user_most_period + "时段提交作业。而大家最常在" + all_most_period + "时段交作业。你还真是错峰上班呢。")
    print("你在" + user_least_period + "时段提交作业最少。")
    print("你最长的一个作业跑了" + str(user_max_runtime_job) + "秒。核时最长的一个作业跑了" + str(user_max_cpu_time_job) + "秒。")
    print("当然你也有眉笔的时候，你有一个作业等待了" + str(user_max_wait_time) + "秒。是今年的历史新高。")
    print(str(int(user_most_date[0:2]))+ "月" + str(int(user_most_date[2:])) + "日你兴致大发，总共交了" +  str(user_most_date_num) + "个作业。是今年之最。")
    print("而" + str(int(user_least_date[0:2])) + "月" + str(int(user_least_date[2:])) + "日你似乎兴致索然，只交了" +  str(user_least_date_num) + "个作业。是今年最少的一天。")
    if username == all_latest_time_user:
        print("你是组里的熬夜之星！你在" + str(int(all_latest_time_user_date[0:2]))+ "月" + str(int(all_latest_time_user_date[2:])) + "日" + \
              str(int(all_latest_time[0:2])) + "时" + str(int(all_latest_time[2:4])) + "分" + str(int(all_latest_time[4:])) + "秒还在交作业！")
    else:
        print(str(int(user_latest_time_date[0:2]))+ "月" + str(int(user_latest_time_date[2:])) + "日" + \
              str(int(user_latest_time[0:2])) + "时" + str(int(user_latest_time[2:4])) + "分" + str(int(user_latest_time[4:])) + "秒你还在交作业。" \
                "但即便如此，你也没能超过组里的熬夜大王，他在" + str(int(all_latest_time[0:2])) + "时" + str(int(all_latest_time[2:4])) + \
                    "分" + str(int(all_latest_time[4:])) + "秒还在交作业。")
    if user_holiday_count == 0:
        print("你居然在" + str(args.year) + "年的假期一个作业都没有交过，是真正的work life balance的化身。")
    else:
        print("你在" + str(args.year) + "年的法定节假日还一共交了" + str(user_holiday_count) + "个作业。我称之为恐怖的内卷王。")
    print("你最常在" + user_most_queue + "队列提交作业。")
    if user_most_software == "others":
        print("鄙人学疏才浅，没能识别出您的任务最常使用什么软件，您可以PR或者接受我诚挚的道歉。(")
    else:
        print("你今年的软件关键词是" + user_most_software + "。")
    print("")
    print("时光荏苒，这一年对于xdft cluster也是波澜不惊的一年。")
    print("今年总共提交了" + str(all_jobs_count) + "个任务，作业运行时长可达" + str(int(all_runtime_sum / 31536000)) + "年，" \
          "CPU核时可达" + str(int(all_cpu_time_sum / 31536000)) + "年。")
    print("今年的平均作业时长是" + str(all_mean_runtime) + "秒，中位数是" + str(all_median_runtime) + "秒。")
    print("今年的平均等待时长是" + str(all_mean_waittime) + "秒，中位数是" + str(all_median_waittime) + "秒。看来也没有那么好等嘛！(")
    print("今年在" + all_most_period + "时段提交作业的人最多。")
    print("今年的交作业之星是" + all_most_jobs_count_user + "。运行时长之星是" + all_most_runtime_user + "。CPU核时之星是" + all_most_cpu_time_user + "。")
    print("今年最长的一个作业跑了" + str(data["all"]["biggest_runtime"]) + "秒。核时最长的一个作业跑了" + str(data["all"]["biggest_cpu_time"]) + "秒。")
    print("今年最长等待时间是" + str(data["all"]["biggest_wait_time"]) + "秒。就不说是哪个幸运儿了，哈哈。")
    print("夜猫子之星是" + all_latest_time_user + "，他在" + str(int(all_latest_time_user_date[0:2]))+ "月" + str(int(all_latest_time_user_date[2:])) + "日" + \
                str(int(all_latest_time[0:2])) + "时" + str(int(all_latest_time[2:4])) + "分" + str(int(all_latest_time[4:])) + "秒还在交作业！")
    print("今年的最常用软件top3是" + all_top_three_software[0] + "、" + all_top_three_software[1] + "和" + all_top_three_software[2] + "。")
    print("今年的最常用队列top3是" + all_top_three_queue[0] + "、" + all_top_three_queue[1] + "和" + all_top_three_queue[2] + "。")
    print("就这样，我们" + str(args.year + 1) + "年再见！")

if __name__ == "__main__":
    main()


        






