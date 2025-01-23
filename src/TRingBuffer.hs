-- | TRingBuffer: A Bounded Concurrent STM-based FIFO queue
--
-- This module is intended to be imported qualified.
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
  { reader :: !(TVar Word)
  , writer :: !(TVar Word)
  , contents :: !(TArray Word a)
  }

instance Show (TRingBuffer a) where
    show buf = "TRingBuffer {capacity = " <> show (capacity buf) <> ", ...}"

-- | Create a new TRingBuffer with the given max `capacity`
empty :: Word -> STM (TRingBuffer a)
empty cap = do
  reader <- TVar.newTVar 0
  writer <- TVar.newTVar 0
  contents <- emptyContents cap
  pure TRingBuffer{reader, writer, contents}

-- | Create a new TRingBuffer, directly in IO (outside of STM)
emptyIO :: Word -> IO (TRingBuffer a)
emptyIO cap = do
  reader <- TVar.newTVarIO 0
  writer <- TVar.newTVarIO 0
  contents <- emptyContents cap
  pure TRingBuffer{reader, writer, contents}

emptyContents :: (MArray a1 e m, Ix i, Integral a2, Num i) => a2 -> m (a1 i e)
emptyContents cap = 
    -- NOTE [capacity]: The size of the allocated array
    -- is one more than the user-specified capacity.
    -- This is necessary because if the buffer is completely full,
    -- we still want the write index to be one less than the read index.
    -- (Otherwise it would be indistinguishable from the 'buffer completely empty' case).
    Array.newArray (0, fromIntegral (cap + 1)) emptyElem

-- | Check the maximum number of elements that can be stored in this TRingBuffer
--
-- Non-blocking. A worst-case O(1) constant-time operation,
-- uses no STM.
capacity :: TRingBuffer a -> Word
capacity buf = 
    let cap = System.IO.Unsafe.unsafePerformIO $ capacity' buf
    in
    -- See NOTE [capacity]
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
-- `pop`/`push`/`tryPop`/`tryPush` (using `STM.retry`)
tryPush :: TRingBuffer a -> a -> STM Bool
tryPush buf a = do
  !cap <- capacity' buf
  readIdx <- TVar.readTVar buf.reader
  writeIdx <- TVar.readTVar buf.writer
  let newWriteIdx = (writeIdx + 1) `mod` cap
  if newWriteIdx == readIdx then
      -- Buffer is full
      pure False
  else do
      Array.writeArray buf.contents writeIdx a
      TVar.writeTVar buf.writer newWriteIdx
      pure True

-- | Attempts to grab the earliest-written element from the TRingBuffer.
-- 
-- Returns `Nothing` if the buffer is empty.
-- Non-blocking. A worst-case O(1) constant-time operation.
--
-- Calls to `tryPop` are synchronized with any other concurrent calls to
-- `pop`/`push`/`tryPop`/`tryPush` (using `STM.retry`)
tryPop :: TRingBuffer a -> STM (Maybe a)
tryPop buf = do
  !cap <- capacity' buf
  readIdx <- TVar.readTVar buf.reader
  writeIdx <- TVar.readTVar buf.writer
  if readIdx == writeIdx then
      -- Buffer is empty
      pure Nothing
  else do
      a <- Array.readArray buf.contents readIdx
      -- NOTE: The next line is not 100% necessary,
      -- but without it, we would hold on to a reference to `a`
      -- until overwritten by a later write, potentially delaying it being GC'd
      Array.writeArray buf.contents readIdx emptyElem
      let newReadIdx = (readIdx + 1) `mod` cap
      TVar.writeTVar buf.reader newReadIdx
      pure (Just a)

-- | Adds a new element to the TRingBuffer.
--
-- If the buffer is full, will block until there is space.
-- (by another thread running `pop` or `tryPop`)
--
-- Calls to `push` are synchronized with any other concurrent calls to
-- `pop`/`push`/`tryPop`/`tryPush` (using `STM.retry`)
push :: TRingBuffer a -> a -> STM ()
push buf a = do
    writingSucceeded <- tryPush buf a
    STM.check writingSucceeded

-- | Reads the oldest element from the TRingBuffer.
--
-- If the buffer is empty, will block until an element was written.
-- (by another thread running `push` or `tryPush`)
--
-- Calls to `pop` are synchronized with any other concurrent calls to
-- `pop`/`push`/`tryPop`/`tryPush` (using `STM.retry`)
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
