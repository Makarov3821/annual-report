import os
import time
import pickle
import argparse


def timestamp_2_mytime(timestamp):
    return time.strftime('%Y,%m,%d,%H,%M,%S', time.localtime(timestamp))

def mytime_2_timestamp(mytime):
    return time.mktime(time.strptime(mytime, '%Y,%m,%d,%H,%M,%S'))

def check_mytime_is_inside(start_time, end_time, target_time):
    if mytime_2_timestamp(start_time) <= mytime_2_timestamp(target_time) <= mytime_2_timestamp(end_time):
        return True
    else:
        return False
    
def check_timestamp_is_inside(start_time, end_time, target_time):
    if start_time <= target_time <= end_time:
        return True
    else:
        return False

def extract_hms_from_timestamp(timestamp):
    return time.strftime('%H%M%S', time.localtime(timestamp))

def extract_md_from_timestamp(timestamp):
    return time.strftime('%m%d', time.localtime(timestamp))
    
def read_in_data(data_file, year):
    year_start_stamp = mytime_2_timestamp(str(year) + ",01,01,00,00,00")
    year_end_stamp = mytime_2_timestamp(str(year) + ",12,31,23,59,59")
    return_list = []
    with open(data_file, 'r', encoding='utf-8', errors='replace') as f:
        data = f.readlines()
    for line in data:
        try:
            soft_mark = 0
            line_split = line.split()
            name = line_split[11].strip('"')
            queue = line_split[12].strip('"')
            timesub_stamp = int(line_split[7])
            timestart_stamp = int(line_split[10])
            if timestart_stamp == 0:
                continue
            timeend_stamp = int(line_split[2])
            cores = int(line_split[6])

            if "g16" in line or "g09" in line or "g03" in line and ".gjf" in line and soft_mark == 0:
                software = "gaussian"
                soft_mark = 1
            elif "vasp" in line and "mpirun" in line and soft_mark == 0:
                software = "vasp"
                soft_mark = 1
            elif "qchem" in line and soft_mark == 0:
                software = "qchem"
                soft_mark = 1
            elif "cp2k" in line and soft_mark == 0:
                software = "cp2k"
                soft_mark = 1
            elif "lmp " in line or "lmp_" in line or "lmp-" in line or "lammps" in line or "LAMMPS" in line and soft_mark == 0:
                software = "lammps"
                soft_mark = 1
            elif "pmemd" in line and soft_mark == 0:
                software = "amber"
                soft_mark = 1
            elif "gmx " in line and soft_mark == 0:
                software = "gromacs"
                soft_mark = 1
            elif "namd2 " in line or "namd3 " in line or "charmrun" in line and soft_mark == 0:
                software = "namd"
                soft_mark = 1
            elif "xtb " in line and soft_mark == 0:
                software = "xtb"
                soft_mark = 1
            elif "ORCA" in line or "orca" in line and "openmpi" in line and soft_mark == 0:
                software = "orca"
                soft_mark = 1
            elif "nwchem " in line and soft_mark == 0:
                software = "nwchem"
                soft_mark = 1
            elif "rest" in line and "rust" in line and soft_mark == 0:
                software = "rest"
                soft_mark = 1
            elif "xcfour" in line and soft_mark == 0:
                software = "cfour"
                soft_mark = 1
            elif "molcas" in line or "Molcas" in line or "pymolcas " in line and soft_mark == 0:
                software = "molcas"
                soft_mark = 1
            elif "molpro" in line and soft_mark == 0:
                software = "molpro"
                soft_mark = 1
            elif "psi4" in line and soft_mark == 0:
                software = "psi4"
                soft_mark = 1
            elif "PYSCF" in line or "pyscf" in line and "python" in line and soft_mark == 0:
                software = "pyscf"
                soft_mark = 1
            elif "aims" in line and soft_mark == 0:
                software = "aims"
                soft_mark = 1
            elif "jdftx" in line and soft_mark == 0:
                software = "jdftx"
                soft_mark = 1
            elif "pw.x" in line or "dos.x" in line or "bands.x" in line or "pp.x" in line and soft_mark == 0:
                software = "quantum espresso"
                soft_mark = 1
            else:
                software = "others"
                soft_mark = 1
            if not check_timestamp_is_inside(year_start_stamp, year_end_stamp, timesub_stamp):
                continue
            #store as [name, queue, timesub_stamp, cores, software, wait_time_s, run_time_s, cpu_time_s]
            return_list.append([name, queue, timesub_stamp, cores, software, timestart_stamp - timesub_stamp, timeend_stamp - timestart_stamp, cores * (timeend_stamp - timestart_stamp)])
        except:
            continue
    return return_list



def main():
    argparser = argparse.ArgumentParser(description='This is a script to analyze the job time of a cluster job.')
    argparser.add_argument('-d', '--dir', help='The directory of the log file.', required=True,dest='dir')
    argparser.add_argument('-y', '--year', help='The year to analyse.', required=True,dest='year', type=int)
    args = argparser.parse_args()

    #first, list all the files in the log dir and read in the data
    raw_data = []
    dir_logs = os.listdir(args.dir)
    print("Finding " + str(len(dir_logs)) + " files in directory. Reading in......")
    for file in dir_logs:
        print("Reading in log " + file)
        raw_data += read_in_data(os.path.join(args.dir, file), args.year)
    print("Done.")

    #read in the holiday file
    holiday_date = []
    with open("holidays.txt", 'r') as f:
        holiday_raw = f.readlines()
    for line in holiday_raw:
        if len(line.split()) == 2:
            if line.split()[0] == str(args.year):
                holiday_date.append(line.split()[1])
        
    #then, analyze the data
    all_dict = {"all": {\
        'jobs_count': 0, 
        'runtime_sum': 0,
        'cpu_time_sum': 0,
        'date': {},
        'queue': {},
        'software': {},
        'latest_time': "000000",
        'latest_time_date': "0101",
        'biggest_runtime': 0,
        'biggest_cpu_time': 0,
        'biggest_wait_time': 0,
        'runtime': [],
        'wait_time': [],
        'time_period': {"1-6": 0, "7-12": 0, "13-18": 0, "19-24": 0}
        }}
    for i in range(len(raw_data)):
        #if the name is not in the dict, add it
        if raw_data[i][0] not in all_dict.keys():
            all_dict[raw_data[i][0]] = {}
            all_dict[raw_data[i][0]]['jobs_count'] = 0
            all_dict[raw_data[i][0]]['runtime_sum'] = 0
            all_dict[raw_data[i][0]]['cpu_time_sum'] = 0
            all_dict[raw_data[i][0]]['date'] = {}
            all_dict[raw_data[i][0]]['queue'] = {}
            all_dict[raw_data[i][0]]['software'] = {}
            all_dict[raw_data[i][0]]['latest_time'] = "000000"
            all_dict[raw_data[i][0]]['latest_time_date'] = "0101"
            all_dict[raw_data[i][0]]['biggest_runtime'] = 0
            all_dict[raw_data[i][0]]['biggest_cpu_time'] = 0
            all_dict[raw_data[i][0]]['biggest_wait_time'] = 0
            all_dict[raw_data[i][0]]['runtime'] = []
            all_dict[raw_data[i][0]]['wait_time'] = []
            all_dict[raw_data[i][0]]['holiday_count'] = 0
            all_dict[raw_data[i][0]]['time_period'] = {
                "1-6": 0, "7-12": 0, "13-18": 0, "19-24": 0}
        
        #add the job count
        all_dict[raw_data[i][0]]['jobs_count'] += 1
        all_dict["all"]['jobs_count'] += 1
        #add the runtime
        all_dict[raw_data[i][0]]['runtime_sum'] += raw_data[i][6]
        all_dict["all"]['runtime_sum'] += raw_data[i][6]
        #add the cpu time
        all_dict[raw_data[i][0]]['cpu_time_sum'] += raw_data[i][7]
        all_dict["all"]['cpu_time_sum'] += raw_data[i][7]
        #add the date
        date = extract_md_from_timestamp(raw_data[i][2])
        if date not in all_dict[raw_data[i][0]]['date'].keys():
            all_dict[raw_data[i][0]]['date'][date] = 1
        else:
            all_dict[raw_data[i][0]]['date'][date] += 1
        if date not in all_dict["all"]['date'].keys():
            all_dict["all"]['date'][date] = 1
        else:
            all_dict["all"]['date'][date] += 1
        #add the queue
        if raw_data[i][1] not in all_dict[raw_data[i][0]]['queue'].keys():
            all_dict[raw_data[i][0]]['queue'][raw_data[i][1]] = 1
        else:
            all_dict[raw_data[i][0]]['queue'][raw_data[i][1]] += 1
        if raw_data[i][1] not in all_dict["all"]['queue'].keys():
            all_dict["all"]['queue'][raw_data[i][1]] = 1
        else:
            all_dict["all"]['queue'][raw_data[i][1]] += 1
        #add the software
        if raw_data[i][4] not in all_dict[raw_data[i][0]]['software'].keys():
            all_dict[raw_data[i][0]]['software'][raw_data[i][4]] = 1
        else:
            all_dict[raw_data[i][0]]['software'][raw_data[i][4]] += 1
        if raw_data[i][4] not in all_dict["all"]['software'].keys():
            all_dict["all"]['software'][raw_data[i][4]] = 1
        else:
            all_dict["all"]['software'][raw_data[i][4]] += 1
        #refresh the latest time
        if int(extract_hms_from_timestamp(raw_data[i][2])) > int(all_dict[raw_data[i][0]]['latest_time']) and int(extract_hms_from_timestamp(raw_data[i][2])) < int("060000"):
            all_dict[raw_data[i][0]]['latest_time'] = extract_hms_from_timestamp(raw_data[i][2])
            all_dict[raw_data[i][0]]['latest_time_date'] = extract_md_from_timestamp(raw_data[i][2])
        if int(extract_hms_from_timestamp(raw_data[i][2])) > int(all_dict["all"]['latest_time']) and int(extract_hms_from_timestamp(raw_data[i][2])) < int("060000"):
            all_dict["all"]['latest_time'] = extract_hms_from_timestamp(raw_data[i][2])
            all_dict["all"]['latest_time_date'] = extract_md_from_timestamp(raw_data[i][2])
        #refresh the biggest runtime
        if raw_data[i][6] > all_dict[raw_data[i][0]]['biggest_runtime']:
            all_dict[raw_data[i][0]]['biggest_runtime'] = raw_data[i][6]
        if raw_data[i][6] > all_dict["all"]['biggest_runtime']:
            all_dict["all"]['biggest_runtime'] = raw_data[i][6]
        #refresh the biggest cpu time
        if raw_data[i][7] > all_dict[raw_data[i][0]]['biggest_cpu_time']:
            all_dict[raw_data[i][0]]['biggest_cpu_time'] = raw_data[i][7]
        if raw_data[i][7] > all_dict["all"]['biggest_cpu_time']:
            all_dict["all"]['biggest_cpu_time'] = raw_data[i][7]
        #refresh the biggest wait time
        if raw_data[i][5] > all_dict[raw_data[i][0]]['biggest_wait_time']:
            all_dict[raw_data[i][0]]['biggest_wait_time'] = raw_data[i][5]
        if raw_data[i][5] > all_dict["all"]['biggest_wait_time']:
            all_dict["all"]['biggest_wait_time'] = raw_data[i][5]
        #add the runtime
        all_dict[raw_data[i][0]]['runtime'].append(raw_data[i][6])
        all_dict["all"]['runtime'].append(raw_data[i][6])
        #add the wait time
        all_dict[raw_data[i][0]]['wait_time'].append(raw_data[i][5])
        all_dict["all"]['wait_time'].append(raw_data[i][5])
        #add the time period
        if 10000 < int(extract_hms_from_timestamp(raw_data[i][2])) < 60000:
            all_dict[raw_data[i][0]]['time_period']["1-6"] += 1
            all_dict["all"]['time_period']["1-6"] += 1
        elif int(extract_hms_from_timestamp(raw_data[i][2])) < 130000:
            all_dict[raw_data[i][0]]['time_period']["7-12"] += 1
            all_dict["all"]['time_period']["7-12"] += 1
        elif int(extract_hms_from_timestamp(raw_data[i][2])) < 190000:
            all_dict[raw_data[i][0]]['time_period']["13-18"] += 1
            all_dict["all"]['time_period']["13-18"] += 1
        else:
            all_dict[raw_data[i][0]]['time_period']["19-24"] += 1
            all_dict["all"]['time_period']["19-24"] += 1
    #now tackle the mean and middle of runtime and wait time
    for user in all_dict.keys():
        tmp_rtime_list = sorted(all_dict[user]["runtime"])
        mean_runtime = int(sum(tmp_rtime_list) / len(tmp_rtime_list))
        if len(tmp_rtime_list) % 2 == 0:
            median_runtime = (tmp_rtime_list[len(tmp_rtime_list) // 2 - 1] + tmp_rtime_list[len(tmp_rtime_list) // 2]) / 2
        else:
            median_runtime = tmp_rtime_list[len(tmp_rtime_list) // 2]
        all_dict[user]["mean_runtime"] = mean_runtime
        all_dict[user]["median_runtime"] = median_runtime

        tmp_wait_list = sorted(all_dict[user]["wait_time"])
        mean_waittime = int(sum(tmp_wait_list) / len(tmp_wait_list))
        if len(tmp_wait_list) % 2 == 0:
            median_waittime = (tmp_wait_list[len(tmp_wait_list) // 2 - 1] + tmp_wait_list[len(tmp_wait_list) // 2]) / 2
        else:
            median_waittime = tmp_wait_list[len(tmp_wait_list) // 2]
        all_dict[user]["mean_waittime"] = mean_waittime
        all_dict[user]["median_waittime"] = median_waittime

        #now find out the most frequent date and least frequent date
        most_freq_date = max(all_dict[user]["date"], key=all_dict[user]["date"].get)
        least_freq_date = min(all_dict[user]["date"], key=all_dict[user]["date"].get)
        all_dict[user]["most_freq_date"] = most_freq_date
        all_dict[user]["least_freq_date"] = least_freq_date

        #now find the date in holidays
        if user == "all":
            continue
        else:
            for date in all_dict[user]["date"]:
                if date in holiday_date:
                    all_dict[user]["holiday_count"] += all_dict[user]["date"][date]
        

        #now remove date, runtime and wait time
        #del all_dict[user]["date"]
        del all_dict[user]["runtime"]
        del all_dict[user]["wait_time"]


    #save the all_dict to a binary file
    with open(str(args.year) + ".bin", 'wb') as f:
        pickle.dump(all_dict, f)

if __name__ == '__main__':
    main()
            



    


