 
import typing
from pplns_types import \
  DataItemWrite, \
  BundleRead

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

def test_input_stream():

  processor = SimpleProcessor()

  stream = api.create_input_stream(
    {
      'consumerId': sink['_id'],
      'taskId': task['_id']
    },
  )

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

def test_prepare_bundle():

  bundle : typing.Any  = \
  {
    '_id': 'something',
    'flowId': 'some_flow_id',
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


