
import datetime
from loguru import logger


'''
Init logger
'''
time_start = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
logger_for_proc_metrics_collector = logger.bind(functional_area="proc_metrics_collector")
logger_for_proc_metrics_collector.add(f"./logs/proc_metrics_collector_{time_start}.log", level="DEBUG", format="{time} - {level} - {message}", rotation="1 week")

time_start = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
logger_for_proc_task_executor = logger.bind(functional_area="proc_task_executor")
logger_for_proc_task_executor.add(f"./logs/proc_task_executor_{time_start}.log", level="DEBUG", format="{time} - {level} - {message}", rotation="1 week")


def utils_execute_cmd_by_ssh(p_ssh_client, p_host_ip, p_cmd, p_logger):
    try:
        stdin, stdout, stderr = p_ssh_client.exec_command(p_cmd)
        ret_stdout, ret_stderr = stdout.read().decode(), stderr.read().decode()
        if ret_stderr:
            raise Exception(ret_stderr)
        p_logger.debug(f'Execute successful: [[host: {p_host_ip}], cmd: {p_cmd}]')
        return ret_stdout
    except Exception as e:
        p_logger.error(f'Execute failed: [[host: {p_host_ip}], cmd: {p_cmd}]:\n    except: {e}')
        return ''




