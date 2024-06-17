
from proc_metrics_collector import ProcMetricsCollector
from proc_task_executor import ProcTaskExecutor


def main():
    # ToDo: Each func should start by single process or thread
    # ProcMetricsCollector().start()
    ProcTaskExecutor().start()


if __name__ == '__main__':
    main()



