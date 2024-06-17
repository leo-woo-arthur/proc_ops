
import os
import time
import yaml
import enum
import numpy
import paramiko

from utils import logger_for_proc_metrics_collector as cur_logger, utils_execute_cmd_by_ssh


class ProcLaunchType(enum.Enum):
    UNKNOWN = 'Unknown'
    NORMAL = 'Normal'
    VALGRIND = 'Valgrind'


class ProcStatusType(enum.Enum):
    UNKNOWN = 'Unknown'
    NORMAL = 'Normal'
    DISCONN = 'Disconnect'
    CRASH = 'Crash'


class ProcMetricPromptMisc:
    def __init__(self, p_mem_keyword, p_normal_includes, p_normal_excludes, p_valgrind_includes, p_valgrind_excludes, p_cmd_top, p_cmd_kill):
        self.mem_keyword = p_mem_keyword
        self.normal_includes = p_normal_includes
        self.normal_excludes = p_normal_excludes
        self.valgrind_includes = p_valgrind_includes
        self.valgrind_excludes = p_valgrind_excludes
        self.cmd_top = p_cmd_top
        self.cmd_kill = p_cmd_kill


class ProcMetricsCollectorCfgMgr:
    def __init__(self, p_cfg_file='proc_metrics_collector_cfg.yaml'):
        self.cfg_file = p_cfg_file

        self.collect_interval = 600        # second
        self.mem_ram_free_th_min = 50000   # KB
        self.username = ''
        self.password = ''
        self.prompt_misc = None
        self.ips_of_proc = []

        self._load_cfg()

    def _load_cfg(self):
        try:
            if not os.path.exists(self.cfg_file):
                raise FileNotFoundError(f"The file [{self.cfg_file}] does not exist!")

            with open(self.cfg_file, 'r') as cfg:
                config = yaml.safe_load(cfg)

            self.collect_interval = config['proc_metrics_collector']['general']['collect_interval']
            self.mem_ram_free_th_min = config['proc_metrics_collector']['general']['min_free_mem']

            self.username = config['proc_metrics_collector']['credentials_of_ssh']['username']
            self.password = config['proc_metrics_collector']['credentials_of_ssh']['password']

            mem_keyword = config['proc_metrics_collector']['prompt_of_proc']['mem_keyword']
            normal_includes = config['proc_metrics_collector']['prompt_of_proc']['launch_by_normal']['includes']
            normal_excludes = config['proc_metrics_collector']['prompt_of_proc']['launch_by_normal']['excludes']
            valgrind_includes = config['proc_metrics_collector']['prompt_of_proc']['launch_by_valgrind']['includes']
            valgrind_excludes = config['proc_metrics_collector']['prompt_of_proc']['launch_by_valgrind']['excludes']
            cmd_top = config['proc_metrics_collector']['prompt_of_cmd']['cmd_top']
            cmd_kill = config['proc_metrics_collector']['prompt_of_cmd']['cmd_kill']
            self.prompt_misc = ProcMetricPromptMisc(mem_keyword, normal_includes, normal_excludes, valgrind_includes, valgrind_excludes, cmd_top, cmd_kill)

            ip_groups_selected = config['proc_metrics_collector']['ips_of_proc']['ip_groups_selected']
            ip_groups_all = config['proc_metrics_collector']['ips_of_proc']['ip_groups_all']
            for cur_group in ip_groups_selected:
                if cur_group in ip_groups_all:
                    self.ips_of_proc.extend(ip_groups_all[cur_group])
                else:
                    cur_logger.warning(f'[ip_groups: {cur_group}] not found in configuration!')

            cur_logger.info(f'CfgMgrForProcMetricsCollector [collect_interval: {self.collect_interval}], [mem_ram_free_th_min: {self.mem_ram_free_th_min}]')
            cur_logger.info(f'CfgMgrForProcMetricsCollector [valgrind_includes: {self.prompt_misc.valgrind_includes}], [valgrind_excludes: {self.prompt_misc.valgrind_excludes}]')
            cur_logger.info(f'CfgMgrForProcMetricsCollector [normal_includes: {self.prompt_misc.normal_includes}], [normal_excludes: {self.prompt_misc.normal_excludes}]')
            cur_logger.info(f'CfgMgrForProcMetricsCollector [mem_keyword: {self.prompt_misc.mem_keyword}]')
            cur_logger.info(f'CfgMgrForProcMetricsCollector [cmd_top: {self.prompt_misc.cmd_top}], [cmd_kill: {self.prompt_misc.cmd_kill}]')
            cur_logger.info(f'CfgMgrForProcMetricsCollector [len_of_ips_of_proc: {len(self.ips_of_proc)}], [ips_of_proc: {self.ips_of_proc}]')
        except FileNotFoundError as e:
            cur_logger.error(f"File Not Found: {e}")
        except yaml.YAMLError as e:
            cur_logger.error(f"Error parsing YAML file: {e}")
        except Exception as e:
            cur_logger.error(f"An unexpected error occurred: {e}")


class ProcMetric:
    def __init__(self, p_ip, p_cmd_prompt):
        self.ip = p_ip
        self.cmd_prompt = p_cmd_prompt

        self.proc_status = ProcStatusType.UNKNOWN
        self.launch_type = ProcLaunchType.UNKNOWN
        self.pid = ''
        self.mem_ram_free = 0
        self.mem_vsz = 0
        self.cpu_pct = 0

        self.pid_change_cnt = 0

    def _update_meter(self, p_proc_status, p_launch_type, p_pid, p_mem_ram_free, p_mem_vsz, p_cpu_pct):
        self.proc_status = p_proc_status
        self.launch_type = p_launch_type

        old_pid = self.pid
        if (p_pid != '') & (old_pid != '') & (p_pid != old_pid):
            self.pid_change_cnt += 1

        if p_pid != '':
            self.pid = p_pid
        self.mem_ram_free = p_mem_ram_free
        self.mem_vsz = p_mem_vsz
        self.cpu_pct = p_cpu_pct

    def _reset_meter(self, p_proc_status=ProcStatusType.UNKNOWN):
        self._update_meter(p_proc_status, ProcLaunchType.UNKNOWN, '', 0, 0, 0)

    def metric_collect(self, ssh_client):
        def closure_extract_metric():
            parts = [cur_part.strip() for cur_part in cur_line.split()]
            part_mem_split = [cur_part.strip() for cur_part in parts[4].split('m') if cur_part]    # parts[4] maybe "998m 54.6" or "1222m149.9", should be split
            parts = parts[:4] + part_mem_split + parts[5:]
            return parts[0], parts[4], parts[7]

        try:
            stdout = utils_execute_cmd_by_ssh(ssh_client, self.ip, self.cmd_prompt.cmd_top, cur_logger)
            ret_launch_type, ret_pid, ret_proc_status, ret_mem_ram_free, ret_mem_vsz, ret_cpu_pct = ProcLaunchType.UNKNOWN, '', '', 0, 0, 0
            for cur_line in stdout.split('\n'):
                if self.cmd_prompt.mem_keyword in cur_line:
                    ret_mem_ram_free = [cur_part.strip() for cur_part in cur_line.split()][3].rstrip('K')    # remove last 'K'
                elif any(include in cur_line for include in self.cmd_prompt.valgrind_includes) and all(exclude not in cur_line for exclude in self.cmd_prompt.valgrind_excludes):    # !!! valgrind must before normal !!!
                    ret_launch_type = ProcLaunchType.VALGRIND
                    ret_pid, ret_mem_vsz, ret_cpu_pct = closure_extract_metric()
                elif any(cur_line.endswith(include) for include in self.cmd_prompt.normal_includes) and all(exclude not in cur_line for exclude in self.cmd_prompt.normal_excludes):
                    ret_launch_type = ProcLaunchType.NORMAL
                    ret_pid, ret_mem_vsz, ret_cpu_pct = closure_extract_metric()

            ret_proc_status = ProcStatusType.NORMAL if ret_pid else ProcStatusType.CRASH
            if ProcStatusType.CRASH == ret_proc_status:
                cur_logger.warning(f"[Host: {self.ip}] cannot find pid, maybe crashed!:\n    {stdout}")

            self._update_meter(ret_proc_status, ret_launch_type, ret_pid, ret_mem_ram_free, ret_mem_vsz, ret_cpu_pct)
        except Exception as e:
            cur_logger.error(f"Failed to get process info of [Host: {self.ip}]: {e}")
            self._reset_meter()    # really need?

    def metric_update_by_disconn(self):
        cur_logger.warning(f"[Host: {self.ip}] cannot connect! reset metric!")
        self._reset_meter(ProcStatusType.DISCONN)


class ProcMetricsCollector:
    def __init__(self):
        self.cfg_mgr = ProcMetricsCollectorCfgMgr()

    def start(self):
        proc_metrics = [ProcMetric(cur_ip, self.cfg_mgr.prompt_misc) for cur_ip in self.cfg_mgr.ips_of_proc]

        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        round_counter = 0
        try:
            while True:
                cur_logger.info('-' * 50 + f'Round: {round_counter}' + '-' * 50)
                round_counter += 1
                for cur_proc_metric in proc_metrics:
                    try:
                        ssh_client.connect(hostname=cur_proc_metric.ip, username=self.cfg_mgr.username, password=self.cfg_mgr.password, timeout=8)
                        cur_proc_metric.metric_collect(ssh_client)
                    except Exception as e:
                        cur_proc_metric.metric_update_by_disconn()
                        cur_logger.error(f"Failed to ssh_client.connect[host: {cur_proc_metric.ip}]:\n    except: {e}")
                    finally:
                        ssh_client.close()

                    cur_logger.info(f"[Host: {cur_proc_metric.ip}], [Status: {cur_proc_metric.proc_status.value}], [PID Change Cnt: {cur_proc_metric.pid_change_cnt}], [Launch: {cur_proc_metric.launch_type.value}], [PID: {cur_proc_metric.pid}], [mem_ram_free: {cur_proc_metric.mem_ram_free}], [mem_vsz: {cur_proc_metric.mem_vsz}], [cpu_pct: {cur_proc_metric.cpu_pct}]")

                    # In valgrind, kill proc if mem is exceed limit and so valgrind can save information
                    if (ProcLaunchType.VALGRIND == cur_proc_metric.launch_type) and (cur_proc_metric.mem_ram_free < self.cfg_mgr.mem_ram_free_th_min) and (cur_proc_metric.pid != ''):
                        cur_logger.info(f'To kill process[{cur_proc_metric.pid}] for mem_ram_free[{cur_proc_metric.mem_ram_free}] < p_mem_ram_free_th_min[{self.cfg_mgr.mem_ram_free_th_min}]')
                        cmd_kill_proc_by_id = f"{self.cfg_mgr.prompt_misc.cmd_kill} {cur_proc_metric.pid}"
                        utils_execute_cmd_by_ssh(ssh_client, cur_proc_metric.ip, cmd_kill_proc_by_id, cur_logger)

                ips_disconn = [cur_proc_metric.ip for cur_proc_metric in proc_metrics if ProcStatusType.DISCONN == cur_proc_metric.proc_status]
                ips_crash = [cur_proc_metric.ip for cur_proc_metric in proc_metrics if ProcStatusType.CRASH == cur_proc_metric.proc_status]
                cur_logger.info(f'Disconnect processes:')
                for cur_ip in ips_disconn:
                    cur_logger.info(f'        [Host: {cur_ip}]')
                cur_logger.info(f'Crash processes:')
                for cur_ip in ips_crash:
                    cur_logger.info(f'        [Host: {cur_ip}]')

                # 计算进程变化的直方图
                cur_logger.info(f'The histogram of process changes')
                pid_change_counts = [metric.pid_change_cnt for metric in proc_metrics]
                histogram_x = [0, 1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 500, 1000, 999999]
                pid_change_counts_histogram = numpy.histogram(pid_change_counts, histogram_x)
                for index in range(len(histogram_x) - 1):
                    cur_logger.info(f'        [{histogram_x[index]:8d} -- {histogram_x[index + 1]:8d}] change counter is [{int(pid_change_counts_histogram[0][index]):12d}]')

                time.sleep(self.cfg_mgr.collect_interval)
        finally:
            ssh_client.close()

