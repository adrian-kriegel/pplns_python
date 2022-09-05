
import threading
import time
import typing

# required to avoid circular dependencies in runtime
if typing.TYPE_CHECKING:
  
  from pplns_python.api import PipelineApi

from pplns_types import \
  BundleQuery, \
  BundleRead, \
  DataItem, \
  DataItemWrite, \
  WorkerWrite

from pplns_python.processor import \
  BatchProcessor, \
  BundleProcessor, \
  PreparedInput

class Stream:

  handlers : dict[str, list[typing.Callable]]

  def __init__(self) -> None:

    self.handlers = {}

  def on(
    self,
    event : str,
    handler : typing.Callable
  ):

    if not event in self.handlers:
      self.handlers[event] = []

    self.handlers[event].append(handler)

    return self


  def close(self) -> None:
    
    self.emit('close')

  def emit(
    self,
    event : str,
    *args : typing.Any
  ) -> typing.Any:
  
    if (not event in self.handlers):
      return

    res = None

    for fnc in self.handlers[event]:

      res = fnc(*args)

    return res

class Interval:

  '''
  Copied from SO, no idea if it works.
  TODO: test
  '''

  def __init__(self, interval, action) -> None:

    self.interval=interval
    self.action=action
    self.stopEvent=threading.Event()
    thread=threading.Thread(target=self.__setInterval)
    thread.start()

  def __setInterval(self) -> None:

    nextTime=time.time()+self.interval

    while not self.stopEvent.wait(nextTime-time.time()):

        nextTime+=self.interval
        self.action()

  def cancel(self)  -> None:

    self.stopEvent.set()

class Counter:

  '''
  Thread safe counter.
  '''

  __max : int
  __counter : int
  __lock : threading.Lock

  def __init__(
    self,
    max : int
  ) -> None:

    self.__max = max
    self.__counter = 0
    self.__lock = threading.Lock()

  def dec(self) -> bool:

    ''' Decrements the counter. Returns true if counter < max.'''

    with self.__lock:
      self.__counter -= 1
    
    return self.__counter < self.__max

  def inc(self) -> bool:

    ''' Increments the counter. Returns true if counter < max.'''

    with self.__lock:
      self.__counter += 1
    
    return self.__counter < self.__max

def prepare_bundle(
  worker : WorkerWrite,
  bundle : BundleRead
) -> PreparedInput:

  '''
  Prepares a bundle to be processed by sorting the data items to match the workers inputs.
  '''

  # first, sort the item references by their position
  item_refs_sorted = sorted(
    bundle['inputItems'],
    key=lambda item : item['position']
  )

  # get the itemIds (because it is tricky to to something as items.find(...) which would be possible in JS)
  item_ids: list[str] = [item['_id'] for item in bundle['items']]

  # sort the actual items by finding the corresponding item to each reference in the sorted references
  items_sorted : list[DataItem] = [
    bundle['items'][item_ids.index(ref['itemId'])] for ref in item_refs_sorted
  ]

  # TODO: this should be done by the core-api!
  # find the largest (deepest) flow stack
  flow_stack_sizes : list[int] = [
    len(item['flowStack']) if 'flowStack' in item else 0 for item in bundle['items']
  ]

  # TODO: this is argmax but ugly ... 
  max_depth: int = max(flow_stack_sizes)
  deepest_item : DataItem = bundle['items'][flow_stack_sizes.index(max_depth)]

  flow_stack = deepest_item['flowStack'] if 'flowStack' in deepest_item else []

  return {
    '_id': bundle['_id'],
    'taskId': bundle['taskId'],
    'flowId': bundle['flowId'],
    'flowStack': flow_stack, 
    'consumerId': bundle['consumerId'],
    'inputs': dict(zip(worker['inputs'].keys(), items_sorted))
  }

class InputStream(Stream):

  '''
  Emits 'data' event when there are new data bundles to be consumed from the api.
  '''

  interval : Interval | None = None

  def __init__(
    self,
    api : 'PipelineApi',
    query : BundleQuery,
    max_concurrency : int = 10,
    polling_time : int = 500,
  ) -> None:

    Stream.__init__(self)

    self.api: 'PipelineApi' = api
    self.query: BundleQuery = query
    self.polling_time: int = polling_time
    self.active_callbacks: Counter = Counter(max=max_concurrency)

    # kill the timer after close
    self.on('close', self.pause)

  def on(
    self,
    event : str,
    callback : typing.Callable
  ) -> Stream:

    '''
    Same as Stream.on but wraps 'data' callbacks in InputStreamDataCallback.
    If the callback raises an Exception, the bundle will be put back to into unconsumed bundles collection.
    '''

    if event == 'data':

      if 'data' in self.handlers:

        raise Exception('InputStream can only have one data callback.')

      else:

        return Stream.on(
          self, 
          event,
          InputStreamDataCallback(self, callback)
        )

    else:

      return Stream.on(self, event, callback)

  def pause(self) -> None:

    ''' Pauses stream. '''

    if self.interval:
      self.interval.cancel()
      self.interval = None

  def start(self) -> None:

    ''' Starts stream if not already started. '''

    if not self.interval and not self.polling_time == -1:
      
      self.interval = Interval(self.polling_time, self.poll)

  def resume(self) -> None:

    ''' Resumes or stars stream. '''

    return self.start()

  def poll(self) -> None:

    '''
    Runs one single polling iteration.
    '''

    bundles: list[BundleRead] = self.api.consume(self.query)
    
    for bundle in bundles:

      self.emit(
        'data',
        prepare_bundle(
          self.api.get_registered_worker(
            bundle['workerId'] if 'workerId' in bundle else None
          ), 
          bundle
        )
      )

  def handle_callback_error(
    self,
    task_id : str,
    bundle_id : str,
    error : Exception
  ) -> None:

    self.api.unconsume(task_id, bundle_id)

    self.emit('error', error)

  def on_data(self, processor : BundleProcessor) -> Stream:

    '''
    Typed alias for on('data', processor).
    '''

    return self.on('data', processor)


class InputStreamDataCallback:

  '''
  Wraps a 'data' callback for an InputStream.
  '''

  def __init__(
    self,
    stream : InputStream,
    processor : BundleProcessor
  ) -> None:

    self.stream: InputStream = stream
    self.processor = processor

  def __call__(self, inp : PreparedInput) -> None:

    # TODO: actually implement batching in the InputStream class
    return self.process_batch([inp])

  def process_batch(self, inputs : list[PreparedInput]) -> None:

    try:

      if not self.stream.active_callbacks.dec():

        self.stream.pause()

      if isinstance(self.processor, BatchProcessor):

        outputs = self.processor(inputs)

      else:

        outputs_or_none = [
          self.processor(inp) for inp in inputs
        ]

        outputs = [o for o in outputs_or_none if o]

      # TODO: this method allow the processor to only populate one output channel
      # TODO: allow the processor to return dict[channel, item]
      if outputs and len(outputs) > 0:

        if not len(outputs) == len(inputs):

          raise Exception(
            'Received {} outputs for {} inputs.'.format(
              len(outputs), len(inputs)
            )
          )

        # TODO: add bulk request feature to API
        for output, bundle in zip(outputs, inputs):

          for channel,o in output.items():
            
            item : DataItemWrite = \
            { 
              **o,
              'outputChannel': channel,
              'done': o['done'] if 'done' in o else True,
              'flowId': bundle['flowId'],
              'flowStack': bundle['flowStack']
            }

            self.stream.api.emit_item(
              {
                'nodeId': bundle['consumerId'],
                'taskId': bundle['taskId'],
              },
              item
            )

    except Exception as e:

      for inp in inputs:

        self.stream.handle_callback_error(
          inp['taskId'],
          inp['_id'],
          e
        )

    finally:

      if self.stream.active_callbacks.inc():

        self.stream.resume()
