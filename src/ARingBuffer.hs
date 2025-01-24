{-# LANGUAGE GHC2021 #-}
{-# LANGUAGE OverloadedRecordDot #-}
module ARingBuffer (
  -- * Definition & creation
  ARingBuffer,
  empty,
  capacity,
  -- * Non-blocking interface
  tryPush,
  tryPop,
  -- * Blocking interface
  push,
  pop
) where

import Control.Concurrent.MVar (MVar)
import Control.Concurrent.MVar qualified as MVar
import Unsafe.SPSCARingBuffer (SPSCARingBuffer)
import Unsafe.SPSCARingBuffer qualified as SPSCARingBuffer

-- | A concurrent FIFO queue, based on atomic counters (hence the 'A')
--
-- If used in a single-producer single-consumer scenario,
-- using `tryPush` and `tryPop` is wait-free.
-- (`push` and `pop` will block, but only when the queue is completely full resp completely empty.)
--
-- Locks (MVars) _are_ used, but only to ensure that only one reader can read at one time,
-- and only one write can write at one time.
--
-- To clarify: readers block other readers, writers block other writers, 
-- but readers never block writers and writers never block readers.
data ARingBuffer a = ARingBuffer
  { readersLock :: {-# UNPACK #-} !Mutex
  , writersLock :: {-# UNPACK #-} !Mutex
  , buffer :: {-# UNPACK #-} !(SPSCARingBuffer a)
  }

-- | Create a new TRingBuffer with the given max `capacity`
empty :: Word -> IO (ARingBuffer a)
{-# INLINE empty #-}
empty cap = do
    readersLock <- newMutex
    writersLock <- newMutex
    buffer <- SPSCARingBuffer.empty cap
    pure (ARingBuffer readersLock writersLock buffer)

-- | Check the maximum number of elements that can be stored in this ARingBuffer
--
-- Non-blocking. A worst-case O(1) constant-time operation
capacity :: ARingBuffer a -> Word
{-# INLINE capacity #-}
capacity buf = SPSCARingBuffer.capacity buf.buffer

-- | Attempts to add a new element to the back of the queue.
--
-- If writing succeeded, returns `True`.
-- If the buffer is full, returns `False`.
--
-- Calls to `push`/`tryPush` are synchronized with each other (and thus wait for each other).
-- Once a thread enters the critical section, 
-- the operation is sure to succeed without other blocks or waits and takes a worst-case O(1) constant-time.
tryPush :: ARingBuffer a -> a -> IO Bool
{-# INLINE tryPush #-}
tryPush buf a = withMutex buf.writersLock $ SPSCARingBuffer.tryPush buf.buffer a

-- | Attempts to read the earliest-written element from the front of the queue
--
-- Returns `Nothing` if the queue is empty.
--
-- Calls to `pop`/`tryPop` are synchronized with each other (and thus wait for each other).
-- Once a thread enters the critical section, 
-- the operation is sure to succeed without other blocks or waits and takes a worst-case O(1) constant-time.
tryPop :: ARingBuffer a -> IO (Maybe a)
{-# INLINE tryPop #-}
tryPop buf = withMutex buf.readersLock $ SPSCARingBuffer.tryPop buf.buffer

-- | Add a new element to the back of the queue
--
-- If the queue is full, will block until another thread calls `pop` or `tryPop`.
--
-- Calls to `push`/`tryPush` are synchronized with each other (and thus wait for each other).
-- Once a thread enters the critical section and the queue is not full,
-- the operation is sure to succeed without other blocks or waits and takes a worst-case O(1) constant-time.
push :: ARingBuffer a -> a -> IO ()
{-# INLINE push #-}
push buf a = withMutex buf.writersLock $ SPSCARingBuffer.push buf.buffer a

-- | Read the earliest-written element from the front of the queue
--
-- If the queue is empty, will block until another thread calls `push` or `tryPush`.
--
-- Calls to `pop`/`tryPop` are synchronized with each other (and thus wait for each other).
-- Once a thread enters the critical section and the queue is not empty,
-- the operation is sure to succeed without other blocks or waits and takes a worst-case O(1) constant-time.
pop :: ARingBuffer a -> IO a
{-# INLINE pop #-}
pop buf = withMutex buf.readersLock $ SPSCARingBuffer.pop buf.buffer


newtype Mutex = Mutex (MVar ())
{-# INLINE newMutex #-}
newMutex :: IO Mutex
newMutex = Mutex <$> MVar.newMVar ()

withMutex :: Mutex -> IO b -> IO b
{-# INLINE withMutex #-}
withMutex (Mutex mut) f = MVar.withMVar mut (\() -> f)
