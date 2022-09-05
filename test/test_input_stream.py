 
from urllib.parse import \
  urlparse,\
  parse_qs

import typing

from pplns_types import \
  DataItemWrite, \
  BundleQuery

from pplns_python.stream import PreparedInput, prepare_bundle

from pplns_python.testing_utils import \
  TestPipelineApi as PipelineApi

from pplns_python.example_worker import example_worker

class SimpleProcessor:

  '''
  Stores all data items in self.inputs
  '''

  def __init__(self):

    self.inputs : list[PreparedInput] = []

  def __call__(self, input : PreparedInput):

    self.inputs.append(input)

class ErrorProcessor:

  '''
  Will simply throw an error when called.
  '''

  def __call__(self, bundle):

    raise Exception('Example Exception.')

def test_input_stream():

  '''
  Test basic input stream behavior.
  '''

  api = PipelineApi()

  task, source, sink = api.utils_source_sink_pipe()

  processor = SimpleProcessor()

  bundle_query : BundleQuery = \
  {
    'consumerId': sink['_id'],
    'taskId': task['_id']
  }

  stream = api.create_input_stream(bundle_query)

  stream.on('data', processor)

  item : DataItemWrite = \
  {
    "outputChannel": 'data',
    "done": True,
    "data": [ 'example data' ],
  }

  emitted_item = api.emit_item(
    { 'nodeId': source['_id'], 'taskId': task['_id'] },
    item
  )

  # calling stream.poll ensures that the data is fetched
  stream.poll()

  stream.close()

  assert len(processor.inputs) == 1
  
  inp = processor.inputs[0]['inputs']

  assert 'in' in inp
  assert inp['in'] == emitted_item

  # check that the queue is now empty

  # check that the item is available to be consumed again
  bundles = api.consume(bundle_query)

  assert len(bundles) == 0


def test_input_stream_error_handling():

  '''
  Tests for error handling in InputStream
  '''

  api = PipelineApi()
  
  task, source, sink = api.utils_source_sink_pipe()

  api.client.clear_logs()

  processor = ErrorProcessor()

  bundle_query : BundleQuery = \
  {
    'consumerId': sink['_id'],
    'taskId': task['_id']
  }

  stream = api.create_input_stream(bundle_query)

  errors : list[Exception] = []

  stream.on('data', processor)
  stream.on('error', lambda e: errors.append(e))

  item : DataItemWrite = \
  {
    "outputChannel": 'data',
    "done": True,
    "data": [ 'example data' ],
  }

  emitted_item = api.emit_item(
    { 'nodeId': source['_id'], 'taskId': task['_id'] },
    item
  )

  # calling stream.poll ensures that the data is fetched
  stream.poll()

  stream.close()

  assert len(errors) == 1
  
  put_requests = api.client.find_requests(
    lambda r: r['method'] == 'put'
  )

  assert len(put_requests) == 1

  # TODO: also check that the correct PUT request was made
  # the assumtion that the request was correct may be made assuming the api client would otherwise
  # forward an Exception from the server

  # check that the item is available to be consumed again
  bundles = api.consume(bundle_query)

  assert len(bundles) == 1

  assert bundles[0]['inputItems'][0]['itemId'] == emitted_item['_id']

def test_prepare_bundle():

  # using Any to be able to only populate fields required by prepare_bundle
  bundle : typing.Any  = \
  {
    '_id': 'something',
    'flowId': 'some_flow_id',
    'taskId': 'some_task_id',
    'consumerId': 'some_consumer_id',
    'inputItems': 
    [
      {
        'position': 1,
        'inputChannel': 'in1',
        'itemId': 'item1',
      },
      {
        'position': 0,
        'inputChannel': 'in0',
        'itemId': 'item0',
      },
      {
        'position': 2,
        'inputChannel': 'in2',
        'itemId': 'item2',
      },  
    ],
    'items': 
    [
      { '_id': 'item2' },
      { '_id': 'item1' },
      { '_id': 'item0' }
    ]
  }

  worker : typing.Any = \
  {
    '_id': 'mock-worker',
    'inputs': 
    {
      'in0': {},
      'in1': {},
      'in2': {}
    }
  }

  prepared: PreparedInput = prepare_bundle(worker, bundle)

  assert prepared['inputs']['in0']['_id'] == 'item0'
  assert prepared['inputs']['in1']['_id'] == 'item1'
  assert prepared['inputs']['in2']['_id'] == 'item2'

def test_emit_returned_items():
  
  '''
  TODO: add test case for MIMO
  '''

  api = PipelineApi()
  
  task, source, sink = api.utils_source_sink_pipe()

  worker = api.register_worker(
    {
      **example_worker,
      'key': 'passthru',
      'inputs': { 'in': {} },
      'outputs': { 'out': {} }
    }
  )

  passthru = api.utils_create_node(
    task,
    {
      'inputs': [
        {
          'nodeId': source['_id'],
          'outputChannel': 'data',
          'inputChannel': 'in',
        }
      ],
      'workerId': worker['_id'],
      'position': { 'x': 0, 'y': 0 }
    }
  )

  # patch the sink node to connect it to the passthru node instead of directly taking inputs from source
  api.patch(
    **api.build_request(
      '/tasks/{}/nodes/{}'.format(task['_id'], sink['_id']),
      { 
        'inputs': 
        [
          {
            'nodeId': passthru['_id'],
            'outputChannel': 'out',
            'inputChannel': 'in',
          }
        ]
      }
    )
  )

  bundle_query : BundleQuery = \
  {
    'consumerId': passthru['_id'],
    'taskId': task['_id']
  }

  stream = api.create_input_stream(
    bundle_query,
    polling_time=-1
  )

  errors = []

  stream.on('error', lambda e: errors.append(e))

  stream.on_data(
    lambda bundle : \
    {
      'out':
      {
        'data': ['processed: ' + bundle['inputs']['in']['data'][0]]
      }
    }
  )

  item : DataItemWrite = \
  {
    "outputChannel": 'data',
    "done": True,
    "data": [ 'example data' ],
  }

  api.emit_item(
    { 'nodeId': source['_id'], 'taskId': task['_id'] },
    item
  )

  bundles = api.get_bundles(bundle_query)
  
  assert len(bundles) == 1
  assert bundles[0]['consumerId'] == passthru['_id']
  

  api.client.clear_logs()

  # calling stream.poll ensures that the data is fetched
  stream.poll()

  stream.close()

  if len(errors) > 0:

    raise errors[0]

  post_requests = api.client.find_requests(
    lambda r: r['method'] == 'post'
  )

  # logs have been cleared after creating the initial item, so there should be only the request
  # to create the item from the passthru node
  assert len(post_requests) == 1

  last_request = api.client.requests[len(api.client.requests) - 1]

  # the last request should be the only post request made
  assert last_request == post_requests[0]

  assert last_request['method'] == 'post'

  url = urlparse(last_request['url'])

  query = parse_qs(url.query)

  assert query['nodeId'] == [passthru['_id']]
  assert query['taskId'] == [task['_id']]

  # TODO: also check url of last request

  bundles = api.consume(
    {
      'consumerId': sink['_id'],
      'taskId': task['_id']
    }
  )

  assert len(bundles) == 1
  assert bundles[0]['items'][0]['data'][0] == 'processed: example data'
