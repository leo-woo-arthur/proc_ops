
import os
import time
import yaml
import enum
import numpy
import queue
import paramiko
from concurrent.futures import ThreadPoolExecutor, as_completed


from utils import logger_for_proc_task_executor as cur_logger, utils_execute_cmd_by_ssh



class ProcTaskExecutorCfgMgr:
    def __init__(self, p_cfg_file='proc_task_executor_cfg.yaml'):
        self.cfg_file = p_cfg_file

        self.max_concurrency = 4
        self.max_retries = 3        #
        self.retry_interval = 1     # second
        self.username = ''
        self.password = ''
        self.tasks = {}
        self.ips_of_proc = []

        self._load_cfg()

    def _load_cfg(self):
        try:
            if not os.path.exists(self.cfg_file):
                raise FileNotFoundError(f"The file [{self.cfg_file}] does not exist!")

            with open(self.cfg_file, 'r') as cfg:
                config = yaml.safe_load(cfg)

            self.max_concurrency = config['proc_task_executor']['general']['max_concurrency']
            self.max_retries = config['proc_task_executor']['general']['max_retries']
            self.retry_interval = config['proc_task_executor']['general']['retry_interval']

            self.username = config['proc_task_executor']['credentials_of_ssh']['username']
            self.password = config['proc_task_executor']['credentials_of_ssh']['password']

            task_selected = config['proc_task_executor']['tasks']['task_selected']
            task_all = config['proc_task_executor']['tasks']['task_all']
            for cur_group in task_selected:
                if cur_group in task_all:
                    self.tasks.update({cur_group: task_all[cur_group]})
                else:
                    cur_logger.warning(f'[task_groups: {cur_group}] not found in configuration!')

            ip_groups_selected = config['proc_task_executor']['ips_of_proc']['ip_groups_selected']
            ip_groups_all = config['proc_task_executor']['ips_of_proc']['ip_groups_all']
            for cur_group in ip_groups_selected:
                if cur_group in ip_groups_all:
                    self.ips_of_proc.extend(ip_groups_all[cur_group])
                else:
                    cur_logger.warning(f'[ip_groups: {cur_group}] not found in configuration!')

            cur_logger.info(f'CfgMgrForProcMetricsCollector [max_concurrency: {self.max_concurrency}], [max_retries: {self.max_retries}], [retry_interval: {self.retry_interval}]')
            cur_logger.info(f'CfgMgrForProcMetricsCollector [tasks: {self.tasks}]')
            cur_logger.info(f'CfgMgrForProcMetricsCollector [len_of_ips_of_proc: {len(self.ips_of_proc)}], [ips_of_proc: {self.ips_of_proc}]')
        except FileNotFoundError as e:
            cur_logger.error(f"File Not Found: {e}")
        except yaml.YAMLError as e:
            cur_logger.error(f"Error parsing YAML file: {e}")
        except Exception as e:
            cur_logger.error(f"An unexpected error occurred: {e}")


class ProcTaskDesc:
    def __init__(self, p_host_ip, p_username, p_password, p_tasks, p_max_retries=3, p_retry_interval=1):
        self.host_ip = p_host_ip
        self.username = p_username
        self.password = p_password
        self.tasks = p_tasks

        self.max_retries = p_max_retries
        self.retry_interval = p_retry_interval


class ProcTaskWorker:
    def __init__(self, p_task_desc):
        self.task_desc = p_task_desc
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.retries = 0

    def is_retryable(self):
        return self.retries < self.task_desc.max_retries

    def execute_commands(self, tasks):
        results = []
        for command in tasks:
            self.client.connect(self.task_desc.host_ip, self.task_desc.username, self.task_desc.password)
            stdin, stdout, stderr = self.client.exec_command(command)
            stdout.channel.recv_exit_status()  # 等待命令执行完成
            results.append({
                'command': command,
                'stdout': stdout.read().decode('utf-8'),
                'stderr': stderr.read().decode('utf-8')
            })
        return results

    def execute(self):
        while not self.is_retryable():
            try:
                stdin, stdout, stderr = self.execute_commands(self.task_desc.tasks)
                stdout.channel.recv_exit_status()
                result = {
                    'hostname': self.task_desc.host_ip,
                    'success': True,
                    'stdout': stdout.read().decode('utf-8'),
                    'stderr': stderr.read().decode('utf-8')
                }
                break
            except Exception as e:
                self.retries += 1
                print(f"Retry {self.retries}/{3} for {self.task_desc.host_ip} due to error: {e}")
                self.close()
            finally:
                if not (self.is_retryable()):
                    result = {
                        'hostname': self.task_desc.host_ip,
                        'success': False,
                        'error': str(e)
                    }
                    break
                self.client.close()
        return result



class ProcTaskWorker:
    def __init__(self, task_desc):
        self.task_desc = task_desc
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.retries = 0

    def is_retryable(self):
        return self.retries < self.task_desc.max_retries

    def connect(self):
        self.client.connect(self.task_desc.host_ip, username=self.task_desc.username, password=self.task_desc.password)

    def execute_commands(self):
        results = []
        try:
            for command in self.task_desc.tasks:
                self.connect()  # 重新连接，以处理可能的网络问题
                stdin, stdout, stderr = self.client.exec_command(command)
                exit_status = stdout.channel.recv_exit_status()
                results.append({
                    'command': command,
                    'exit_status': exit_status,
                    'stdout': stdout.read().decode('utf-8'),
                    'stderr': stderr.read().decode('utf-8')
                })
                if exit_status != 0:
                    break  # 如果命令执行失败，则中断后续命令的执行
        except Exception as e:
            print(f"Error executing commands on {self.task_desc.host_ip}: {e}")
            results = {'error': str(e)}
        finally:
            self.client.close()
        return results

    def execute(self):
        while self.is_retryable():
            results = self.execute_commands()
            if isinstance(results, dict) and results.get('error'):
                # 记录重试次数，然后等待一段时间后重试
                self.retries += 1
                print(f"Retry {self.retries}/{self.task_desc.max_retries} for {self.task_desc.host_ip} due to error: {results['error']}")
                time.sleep(self.task_desc.retry_interval)
            else:
                # 命令执行成功或出现异常但不可重试
                break
        return {
            'hostname': self.task_desc.host_ip,
            'success': not isinstance(results, dict) or not results.get('error'),
            'results': results
        }


class ProcTaskMaster:
    def __init__(self, p_tasks_desc, p_max_concurrency):
        self.tasks_desc = p_tasks_desc
        self.max_concurrency = p_max_concurrency

        self.results = {}

    def run(self):
        workers = [ProcTaskWorker(cur_task_desc) for cur_task_desc in self.tasks_desc]

        with ThreadPoolExecutor(max_workers=self.max_concurrency) as executor:
            futures_workers = {executor.submit(cur_worker.execute): cur_worker for cur_worker in workers}

        for cur_future_worker in as_completed(futures_workers):
            worker = futures_workers[cur_future_worker]
            result = cur_future_worker.result()
            self.results[worker.task_desc.host_ip] = result


class ProcTaskExecutor:
    def __init__(self):
        self.cfg_mgr = ProcTaskExecutorCfgMgr()

    def start(self):

        tasks_desc = []
        for cur_ip in self.cfg_mgr.ips_of_proc:
            task_desc = ProcTaskDesc(cur_ip, self.cfg_mgr.username, self.cfg_mgr.password, self.cfg_mgr.tasks, self.cfg_mgr.max_retries, self.cfg_mgr.retry_interval)
            tasks_desc.append(task_desc)

        proc_task_master = ProcTaskMaster(tasks_desc, self.cfg_mgr.max_concurrency)
        proc_task_master.run()


