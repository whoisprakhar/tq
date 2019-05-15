import click

from tq.connection import rc
from tq.logger import info
from tq.worker import Worker, ScheduledWorker, FailedWorker


default_queues = 'main'

queue_help = 'Queues for worker, separated by commas'
scheduled_worker_help = 'Scheduled worker'
failed_worker_help = 'Failed worker'


@click.command()
@click.option('--queues', type=click.STRING, required=False, help=queue_help,
              default=default_queues)
@click.option('--scheduled', type=click.BOOL, default=False,
              help=scheduled_worker_help)
@click.option('--failed', type=click.BOOL, default=False,
              help=failed_worker_help)
def tq_manager(queues, scheduled, failed):
    worker_klass = Worker
    queues = queues.split(',')

    if scheduled or failed:
        worker_klass = ScheduledWorker if scheduled else FailedWorker

    info('{}{}'.format('Starting worker for ', queues))
    info('Waiting for jobs.')
    info('Worker type {}\n'.format(worker_klass.__name__))

    worker = worker_klass(queues, rc)
    worker.run()


if __name__ == '__main__':
    tq_manager()
