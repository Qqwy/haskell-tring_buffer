{-# LANGUAGE GHC2021 #-}
{-# LANGUAGE OverloadedRecordDot #-}
module Unsafe.SPSCARingBuffer (
  -- * Definition & creation
  SPSCARingBuffer,
  empty,
  capacity,
  -- * Non-blocking interface
  tryPush,
  tryPop,
  -- * Blocking interface
  push,
  pop
) where

import Control.Monad.Primitive (RealWorld)
import Data.Primitive.Array (MutableArray)
import Data.Primitive.Array qualified as Array
import Control.Concurrent.Counter (Counter)
import Control.Concurrent.Counter qualified as Counter
import Control.Concurrent.MVar (MVar)
import Control.Concurrent.MVar qualified as MVar

-- | A concurrent single-producer, single-consumer FIFO queue, based on atomic counters
--
-- It is **unsafe** to have multiple concurrent readers. This is not checked.
-- It is **unsafe** to have multiple concurrent writers. This is not checked.
--
-- Skipping these checks allows this abstraction to be
-- completely lock-free and wait-free in `tryPush` and `tryPop`.
-- (`push` and `pop` will block, but only when the queue is completely full resp completely empty.)
--
-- If you want to potentially have multiple concurrent readers or writers,
-- consider MPMCRingBuffer instead, which uses an extra 'reader-lock' and 'writer-lock'.
data SPSCARingBuffer a = SPSCARingBuffer
  { reader :: {-# UNPACK #-} !Counter -- ^ The next index to read from
  , writer :: {-# UNPACK #-} !Counter -- ^ The next index to write to
  , contents :: {-# UNPACK #-} !(MutableArray RealWorld a) -- ^ The buffer itself
  , emptySigSem :: {-# UNPACK #-} !BinSem -- ^ Wait when empty, be notified when a push comes in
  , fullSigSem :: {-# UNPACK #-} !BinSem -- ^ Wait when full, be notified when a pop comes in
  }

empty :: Word -> IO (SPSCARingBuffer a)
{-# INLINE empty #-}
empty cap = do
  reader <- Counter.new 0
  writer <- Counter.new 0
  contents <- emptyContents
  emptySigSem <- newBinSem
  fullSigSem <- newBinSem
  pure (SPSCARingBuffer reader writer contents emptySigSem fullSigSem)
  where
    emptyContents = Array.newArray (fromIntegral $ cap + 1) emptyElem

capacity :: SPSCARingBuffer a -> Word
{-# INLINE capacity #-}
capacity buf = (fromIntegral $ capacity' buf) - 1

capacity' :: SPSCARingBuffer a -> Int
{-# INLINE capacity' #-}
capacity' buf = Array.sizeofMutableArray buf.contents

-- | Attempts to add a new element to the buffer.
--
-- If writing succeeded, returns `True`.
-- If the buffer is full, returns `False`.
--
-- Non-blocking and wait-free. A worst-case O(1) constant-time operation.
tryPush :: SPSCARingBuffer a -> a -> IO Bool
{-# INLINE tryPush #-}
tryPush buf a = do
  let !cap = capacity' buf
  writeIdx <- Counter.get buf.writer
  let !newWriteIdx = circularInc writeIdx cap
  readIdx <- Counter.get buf.reader
  if newWriteIdx == readIdx then
    pure False
  else do
    Array.writeArray buf.contents writeIdx a
    Counter.set buf.writer newWriteIdx
    notifyBinSem buf.emptySigSem
    pure True

-- | Attempts to grab the earliest-written element from the buffer.
-- 
-- Returns `Nothing` if the buffer is empty.
-- Non-blocking and wait-free. A worst-case O(1) constant-time operation.
tryPop :: SPSCARingBuffer a -> IO (Maybe a)
{-# INLINE tryPop #-}
tryPop buf = do
  let !cap = capacity' buf
  readIdx <- Counter.get buf.reader
  writeIdx <- Counter.get buf.writer
  if readIdx == writeIdx then
    pure Nothing
  else do
    a <- Array.readArray buf.contents readIdx
    -- Array.writeArray buf.contents readIdx emptyElem -- TODO: Doublecheck
    let newReadIdx = circularInc readIdx cap
    Counter.set buf.reader newReadIdx
    notifyBinSem buf.fullSigSem 
    pure (Just a)

-- | Add a new element to the back of the queue
--
-- If the queue is not full, it will not block and takes a worst-case O(1) constant-time.
-- If the queue is full, will block until another thread calls `pop` or `tryPop`.
push :: SPSCARingBuffer a -> a -> IO ()
{-# INLINE push #-}
push buf a = do
  res <- tryPush buf a
  if res then 
    pure () 
  else do
    -- If full, wait until no longer full and try again
    waitBinSem buf.fullSigSem
    push buf a

-- | Read the earliest-written element from the front of the queue
--
-- If the queue is non-empty, will not block and takes a worst-case O(1) constant-time.
-- If the queue is empty, will block until another thread calls `push` or `tryPush`.
pop :: SPSCARingBuffer a -> IO a
{-# INLINE pop #-}
pop buf = do
  res <- tryPop buf
  case res of
    Just a -> pure a
    Nothing -> do
      -- If empty, wait until no longer empty and try again
      waitBinSem buf.emptySigSem
      pop buf


circularInc :: Integral a => a -> a -> a
{-# INLINE circularInc #-}
circularInc x cap = (x + 1) `mod` cap

emptyElem :: a
{-# INLINE emptyElem #-}
emptyElem = (error "attempted to read an uninitialized element of an SPSCARingBuffer. This should be impossible, and thus indicates a bug in SPSCARingBuffer.")

-- | A binary semaphore
newtype BinSem = BinSem (MVar ())

newBinSem :: IO BinSem
{-# INLINE newBinSem #-}
newBinSem = BinSem <$> MVar.newEmptyMVar

waitBinSem :: BinSem -> IO ()
{-# INLINE waitBinSem #-}
waitBinSem (BinSem sem) = MVar.takeMVar sem

notifyBinSem :: BinSem -> IO ()
{-# INLINE notifyBinSem #-}
notifyBinSem (BinSem sem) = do 
  _ <- MVar.tryPutMVar sem ()
  pure ()
