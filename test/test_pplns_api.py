
from urllib.parse import ParseResult, urlparse

from pplns_python.testing_utils import \
  TestPipelineApi as PipelineApi

from pplns_python.example_worker import example_worker

from pplns_types import \
  DataItemWrite  


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


def test_register_worker() -> None:

  api = PipelineApi()

  task, source, sink = api.utils_source_sink_pipe()

  result = api.register_worker(
    example_worker
  )

  assert result['_id']

  assert isinstance(api.workers[result['_id']], dict)

def test_emit_item_consume_item():
  
  api = PipelineApi()

  task, source, sink = api.utils_source_sink_pipe()

  item : DataItemWrite = \
  {
    "outputChannel": 'data',
    "done": True,
    "data": [ 'example data' ],
    "consumptionId": None,
  }

  emit_response = api.emit_item(
    { 'nodeId': source['_id'], 'taskId': task['_id'] },
    item
  )

  # TODO: check response

  # prepare by registering a task 

  result = api.consume(
    { 
      'consumerId': sink['_id'],
      'taskId': task['_id'],
    }
  )

  assert isinstance(result, list)

  assert len(result) == 1

  bundle = result[0]

  input_items = bundle['items']
  
  assert len(input_items) == 1

  assert input_items[0]['_id'] == emit_response['_id']
  assert input_items[0]['data'][0] == item['data'][0]

  # TODO: since the behavior of the API is tested, it is enough to check that the request made
  # matches the expected request
  





