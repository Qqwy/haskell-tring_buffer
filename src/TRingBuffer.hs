-- | TRingBuffer: A Bounded Concurrent STM-based FIFO queue
--
-- This module is intended to be imported qualified.
--
-- ## Concurrency guarantees
--
-- TRingBuffer is built on top of STM, which does optimistic concurrency:
-- Concurrent threads execute at the same time, and when a conflict between two concurrent threads is detected, 
-- only one is allowed to continue, with the other one automatically retrying.
--
-- Ring buffers have one 'read index' and one 'write index'.
-- - During a 'push', only the 'write index' is updated, but we check against the 'read index' to ensure we don't try to push to a full buffer.
-- - During a 'pop', only the 'read index' is updated, but we check against the 'write index' to ensure we don't try to read from an empty buffer.
--
-- To reduce the amount of STM read-write conflicts, we cache the last-seen 'read index' in the TVar that contains the most up-to-date 'write index',
-- and cache the last-seen 'write index' in the TVar that contains the most up-to-date 'read index'.
--
-- This way, we can skip checking the other TVar if we're sure there is enough space (when writing) or enough elements (when reading).
-- That, in turn, allows us to most of the time allow reads to continue concurrently with writes (and vice-versa, vacuously).
--
-- Only when we're scared that there _may_ be a conflict, do we check against the other TVar, and then immediately update our cached value with the result.
--
-- Assuming pushes and pops happen at the same rate on average, this means that only one every `capacity / 2` operations
-- will have to check both TVars at the same time (and conflict with any possible concurrent reads _and_ writes).
{-# LANGUAGE GHC2021 #-}
{-# LANGUAGE OverloadedRecordDot #-}
module TRingBuffer 
( 
    -- * Definition & Creation
    TRingBuffer
    , empty
    , emptyIO
    , capacity
    -- * Blocking interface
    , push
    , pop
    -- * Non-blocking interface
    , tryPush
    , tryPop
)
where

import Control.Concurrent.STM (STM)
import Control.Concurrent.STM qualified as STM
import Control.Concurrent.STM.TVar (TVar)
import Control.Concurrent.STM.TVar qualified as TVar
-- import Data.Primitive.Array (MutableArray)
-- import Data.Primitive.Array qualified as Array
-- import Control.Monad.Primitive (RealWorld)
import Control.Concurrent.STM.TArray (TArray)
-- import Control.Concurrent.STM.TArray qualified as TArray
import Data.Array.Base (MArray)
import Data.Ix (Ix)
import Data.Array.Base qualified as Array
-- import Data.Array.MArray (MArray)
-- import Data.Array.MArray qualified as MArray
import System.IO.Unsafe qualified


-- | A Bounded Concurrent STM-based FIFO Queue, implemented as a Ring Buffer
--
-- Goals of this datastructure:
-- - Easy to use; no hidden footguns
-- - Predictable memory usage
-- - O(1) pushing and O(1) popping. These are real-time (i.e. worst-case) bounds, no amortization!
-- - Simple implementation
--
-- In essence, this is a Haskell translation of the traditional
-- 'ring buffer', AKA 'circular buffer' as described for example
-- - https://en.wikipedia.org/wiki/Circular_buffer
-- - https://rigtorp.se/ringbuffer/
data TRingBuffer a = TRingBuffer
  { readerAndCachedWriter :: !(TVar Indexes)
  , writerAndCachedReader :: !(TVar Indexes)
  , contents :: !(TArray Word a)
  }

data Indexes = Indexes
  {-# UNPACK #-} !Word  -- ^ An up-to-date value
  {-# UNPACK #-} !Word -- ^A cached value, updated only when needed

instance Show (TRingBuffer a) where
    show buf = "TRingBuffer {capacity = " <> show (capacity buf) <> ", ...}"

-- | Create a new TRingBuffer with the given max `capacity`
empty :: Word -> STM (TRingBuffer a)
empty cap = do
  let readIdx = 0
  let writeIdx = 0
  readerAndCachedWriter <- TVar.newTVar (Indexes readIdx writeIdx)
  writerAndCachedReader <- TVar.newTVar (Indexes writeIdx readIdx)
  contents <- emptyContents cap
  pure TRingBuffer{readerAndCachedWriter, writerAndCachedReader, contents}

-- | Create a new TRingBuffer, directly in IO (outside of STM)
emptyIO :: Word -> IO (TRingBuffer a)
emptyIO cap = do
  let readIdx = 0
  let writeIdx = 0
  readerAndCachedWriter <- TVar.newTVarIO (Indexes readIdx writeIdx)
  writerAndCachedReader <- TVar.newTVarIO (Indexes writeIdx readIdx)
  contents <- emptyContents cap
  pure TRingBuffer{readerAndCachedWriter, writerAndCachedReader, contents}

emptyContents :: (MArray a1 e m, Ix i, Integral a2, Num i) => a2 -> m (a1 i e)
emptyContents cap = 
    -- NOTE [capacity]: The size of the allocated array
    -- is one more than the user-specified capacity.
    -- This is necessary because if the buffer is completely full,
    -- we still want the write index to be one less than the read index.
    -- (Otherwise it would be indistinguishable from the 'buffer completely empty' case).
    Array.newArray (0, fromIntegral cap) emptyElem

-- | Check the maximum number of elements that can be stored in this TRingBuffer
--
-- Non-blocking. A worst-case O(1) constant-time operation,
-- uses no STM.
capacity :: TRingBuffer a -> Word
capacity buf = 
    -- SAFETY: Once the array is allocated,
    -- we never grow or shrink it,
    -- so this call is outwardly pure
    let cap = System.IO.Unsafe.unsafePerformIO $ capacity' buf
    in -- See NOTE [capacity]
      cap - 1

-- Returns the _true_ capacity, including the extra element
capacity' :: MArray TArray a m => TRingBuffer a -> m Word
capacity' buf = fromIntegral <$> Array.getNumElements buf.contents

-- | Attempts to add a new element to the TRingBuffer.
--
-- If writing succeeded, returns `True`.
-- If the buffer is full, returns `False`.
--
-- Non-blocking. A worst-case O(1) constant-time operation.
--
-- Calls to `tryPush` are synchronized with any other concurrent calls to
-- `push`/`tryPush` (using `STM.retry`)
--
-- Most of the time, pushing can happen concurrently with popping.
-- Assuming pushes and pops happen at the same rate (on average),
-- only one every `capacity / 2` pushes will synchronize with a pop.
tryPush :: TRingBuffer a -> a -> STM Bool
tryPush buf a = do
  !cap <- capacity' buf
  Indexes writeIdx readIdxCached <- TVar.readTVar buf.writerAndCachedReader
  let newWriteIdx = (writeIdx + 1) `mod` cap
  if newWriteIdx == readIdxCached then do
    -- Buffer may be full, double-check with real read index
    Indexes readIdxFresh _ <- TVar.readTVar buf.readerAndCachedWriter
    if newWriteIdx == readIdxFresh then
      -- Buffer is truly full, nothing more to do
      -- (If the caller calls `STM.retry` after this point, any `tryPop` will trigger a transaction re-try!)
      pure False
    else do
      -- Buffer not full after all.
      -- Save value, update write idx 
      -- and also update cached read idx
      actuallyPush writeIdx newWriteIdx readIdxFresh
  else do
      -- There is certainly space
      -- Save value, update write idx
      actuallyPush writeIdx newWriteIdx readIdxCached
  where
    actuallyPush writeIdx newWriteIdx readIdx = do
      Array.writeArray buf.contents writeIdx a
      TVar.writeTVar buf.writerAndCachedReader (Indexes newWriteIdx readIdx)
      pure True

-- | Attempts to grab the earliest-written element from the TRingBuffer.
-- 
-- Returns `Nothing` if the buffer is empty.
-- Non-blocking. A worst-case O(1) constant-time operation.
--
-- Calls to `tryPop` are synchronized with any other concurrent calls to
-- `pop`/`tryPop` (using `STM.retry`)
--
-- Most of the time, popping can happen concurrently with pushing.
-- Assuming pushes and pops happen at the same rate (on average),
-- only one every `capacity / 2` pops will synchronize with a push.
tryPop :: TRingBuffer a -> STM (Maybe a)
tryPop buf = do
  Indexes readIdx writeIdxCached <- TVar.readTVar buf.readerAndCachedWriter
  if readIdx == writeIdxCached then do
      -- Buffer may be empty, double-check with real write index
      Indexes writeIdxFresh _ <- TVar.readTVar buf.writerAndCachedReader
      if readIdx == writeIdxFresh then do
        -- Buffer is truly empty, nothing more to do
        -- (If the caller calls `STM.retry` after this point, any `tryPush` will trigger a transaction re-try!)
        pure Nothing
      else do
        -- Buffer not empty after all.
        -- Extract value, update read idx
        -- and also update cached write idx
        actuallyPop readIdx writeIdxFresh
  else do
    -- Buffer definitely non-empty
    -- Extract value, (only) update read idx
    actuallyPop readIdx writeIdxCached
  where
    actuallyPop readIdx writeIdx = do
      a <- Array.readArray buf.contents readIdx
      -- NOTE: The next line is not 100% necessary,
      -- but without it, we would hold on to a reference to `a`
      -- until overwritten by a later write, potentially delaying it being GC'd
      Array.writeArray buf.contents readIdx emptyElem
      !cap <- capacity' buf
      let newReadIdx = (readIdx + 1) `mod` cap
      TVar.writeTVar buf.readerAndCachedWriter (Indexes newReadIdx writeIdx)
      pure (Just a)

-- | Adds a new element to the TRingBuffer.
--
-- If the buffer is full, will block until there is space.
-- (by another thread running `pop` or `tryPop`)
--
-- Calls to `push` are synchronized with any other concurrent calls to
-- `push`/`tryPush` (using `STM.retry`)
--
-- Most of the time, pushing can happen concurrently with popping.
-- Assuming pushes and pops happen at the same rate (on average),
-- only one every `capacity / 2` pushes will synchronize with a pop.
push :: TRingBuffer a -> a -> STM ()
push buf a = do
    res <- tryPush buf a
    case res of
        False -> STM.retry
        True -> pure ()

-- | Reads the oldest element from the TRingBuffer.
--
-- If the buffer is empty, will block until an element was written.
-- (by another thread running `push` or `tryPush`)
--
-- Calls to `pop` are synchronized with any other concurrent calls to
-- `pop`/`tryPop` (using `STM.retry`)
--
-- Most of the time, popping can happen concurrently with pushing.
-- Assuming pushes and pops happen at the same rate (on average),
-- only one every `capacity / 2` pops will synchronize with a push.
pop :: TRingBuffer a -> STM a
pop buf = do 
    res <- tryPop buf
    case res of
        Nothing -> STM.retry
        Just a -> pure a

-- Used a single shared 'default' element
-- that is used for empty spots in the array.
--
-- This reduces a level of Pointer indirection vs storing `Maybe a` inside the array
emptyElem :: a
emptyElem = (error "attempted to read an uninitialized element of a TRingBuffer. This should be impossible, and thus indicates a bug in TRingBuffer.")
