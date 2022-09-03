 
from distutils import errors
import typing

from pplns_types import \
  DataItemWrite, \
  BundleQuery

from pplns_python.input_stream import PreparedInput, prepare_bundle

from test.testing_utils import \
  TestPipelineApi as PipelineApi


api = PipelineApi()

task, source, sink = api.utils_source_sink_pipe()

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


