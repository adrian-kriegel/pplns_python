
import os

from pplns_python.api import PipelineApi

import typing

from pplns_types import \
  NodeWrite, \
  NodeRead, \
  Task, \
  NodeWrite, \
  Worker

def env(s : str) -> str:

  v: str | None  = os.environ.get(s)

  if v:
    return v
  else:
    raise Exception('Missing environment variable: ' + s)

source_node : NodeWrite = \
{
  'inputs': [],
  'internalWorker': 'data-source',
  'position': { 'x': 0, 'y': 0 }
}

def sink_node(source_node : NodeRead) -> NodeWrite:

  return {
    'inputs': [
      {
        'nodeId': source_node['_id'],
        'inputChannel': 'in',
        'outputChannel': 'data'
      }
    ],
    'internalWorker': 'data-sink',
    'position': { 'x': 0, 'y': 0 }
  }

class TestPipelineApi(PipelineApi):

  def __init__(self, url : typing.Optional[str] = None) -> None:

    PipelineApi.__init__(self, url or env('PPLNS_API'))


  def get_registered_worker(self, workerId: typing.Optional[str]) -> Worker:
    
    if not workerId:

      worker : Worker = {
        'key': 'mock-worker',
        '_id': 'mock-worker',
        'title': '',
        'description': '',
        'params': {},
        'createdAt': '',
        'inputs': { 'in': { } },
        'outputs': { 'data': {} }
      }

      return worker

    else:

      return self.workers[workerId]

  def utils_create_task(
    self,
  ) -> Task:

    return self.post(
      **self.build_request(
        '/tasks',
        {
          'title': 'test task',
          'description': 'nothing here',
          'params': {},
          'owners': []
        }
      )
    )

  def utils_create_node(
    self,
    task : Task,
    node : NodeWrite
  ) -> NodeRead:

    task_id = task['_id']

    return self.post(
      **self.build_request(
        f'/tasks/{task_id}/nodes',
        node,
      )
    )

  def utils_source_sink_pipe(
    self
  ) -> tuple[Task, NodeRead, NodeRead]:

    '''
    Creates new task with simple test pipeline setup.

    source -> sink

    returns task, source, sink
    '''

    task = self.utils_create_task()

    source = self.utils_create_node(
      task,
      source_node,
    )

    sink = self.utils_create_node(
      task,
      sink_node(source),
    )

    return task, source, sink


def cleanup():

  # TODO: move to env and then CHECK THAT mongo_url IS LOCAL AND MEANT FOR TESTING
  mongo_url : str = 'mongodb://unit_test:123@127.0.0.1:27017/unit_test'

  command : str = f'mongo {mongo_url} --eval "db.dropDatabase()"'

  os.system(command)


cleanup()