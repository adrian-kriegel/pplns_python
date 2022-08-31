
import os
from urllib.parse import ParseResult, urlparse

from pplns_python.api import PipelineApi
from pplns_python.example_worker import example_worker

from pplns_types import TaskWrite

def env(s : str) -> str:

  v: str | None  = os.environ.get(s)

  if v:
    return v
  else:
    raise Exception('Missing environment variable: ' + s)

def test_build_uri() -> None:

  uri: str = PipelineApi('http://example.com/api').build_uri(
    'path/to/resource',
    { 'foo': 'bar', 'test': 'bart' }
  )

  parsed: ParseResult = urlparse(uri)

  assert parsed.scheme == 'http'
  assert parsed.netloc == 'example.com'
  assert parsed.path == '/api/path/to/resource'
  assert parsed.query == 'foo=bar&test=bart'

  assert uri == 'http://example.com/api/path/to/resource?foo=bar&test=bart'


api = PipelineApi(env('PPLNS_API'))

def test_register_worker() -> None:

  result = api.register_worker(
    example_worker
  )

  assert result['_id']

  assert isinstance(api.workers[result['_id']], dict)

def test_consume():

  # prepare by registering a task 

  task : TaskWrite = {
    'title': 'test task',
    'description': 'nothing here',
    'params': {},
    'owners': []
  }

  task_id = api.post(
    **api.build_request(
      api.build_uri('/tasks'),
      task
    )
  )['_id']

  workerId : str = list(api.workers.keys())[0]

  result = api.consume(
    { 
      'workerId': workerId,
      'taskId': task_id,
      # TODO: this is required until typing.NotRequired is introduced in python 3.11
      '_id': None,
      'consumerId': None,
      'done': True,
      'flowId': None,
      'limit': 1,
      'consume': True,
    }
  )

  assert isinstance(result, list)

  # TODO: since the behavior of the API is tested, it is enough to check that the request made
  # matches the expected request