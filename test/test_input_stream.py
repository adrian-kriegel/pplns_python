 
from pplns_types import DataItemWrite
from pplns_python.input_stream import PreparedInput

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



