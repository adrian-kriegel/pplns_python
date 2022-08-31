
import threading
from time import time
import typing


# required to avoid circular dependencies in runtime
if typing.TYPE_CHECKING:
  
  from pplns_python.api import PipelineApi

from pplns_types import \
  BundleQuery, \
  BundleRead, \
  DataItem, \
  DataItemWrite, \
  FlowIdSchema, \
  WorkerWrite

PreparedInput = typing.TypedDict(
  'PreparedInput',
  {
    # bundle id
    '_id': str,
    # bundle flowId
    'flowId': FlowIdSchema,
    # data items by their name
    'inputs': dict[str, DataItem],
  },
)

BundleProcessor = typing.Callable[
  [PreparedInput],
  list[DataItemWrite] | DataItemWrite
]

class Stream:

  handlers : dict[str, list[typing.Callable]]

  def on(
    self,
    event : str,
    handler : typing.Callable
  ):

    if not event in self.handlers:
      self.handlers[event] = []

    self.handlers[event].append(handler)

    return self

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

  return {
    '_id': bundle['_id'],
    'flowId' :bundle['flowId'],
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

    self.api: 'PipelineApi' = api
    self.query: BundleQuery = query
    self.polling_time: int = polling_time
    self.active_callbacks: Counter = Counter(max=max_concurrency)

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

    if not self.interval:
      
      self.interval = Interval(self.polling_time, self.__poll)

  def resume(self) -> None:

    ''' Resumes or stars stream. '''

    return self.start()

  def __poll(self) -> None:

    '''
    Runs one single polling iteration.
    '''

    bundles: list[BundleRead] = self.api.consume(self.query)

    for bundle in bundles:

      self.emit(
        'data',
        prepare_bundle(
          self.api.workers[bundle['workerId']], 
          bundle
        )
      )

  def __handle_callback_error(
    self,
    bundleId : str,
    error : Exception
  ) -> None:

    self.api.unconsume(bundleId)

    self.emit('error', error)


class InputStreamDataCallback:

  '''
  Wraps a 'data' callback for an InputStream.
  '''

  def __init__(
    self,
    stream : InputStream,
    fnc : BundleProcessor
  ) -> None:

    self.stream: InputStream = stream
    self.fnc = fnc

  def __call__(self, input : PreparedInput) -> None:

    try:

      if not self.stream.active_callbacks.dec():

        self.stream.pause()

      self.fnc(input)

    except Exception as e:

      self.stream.__handle_callback_error(input['_id'], e)

    finally:

      if self.stream.active_callbacks.inc():

        self.stream.resume()
