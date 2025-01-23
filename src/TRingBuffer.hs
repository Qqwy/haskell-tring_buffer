{-# LANGUAGE GHC2021 #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE UndecidableInstances #-}
module TRingBuffer 
( 
    -- * Definition & Creation
    TRingBuffer
    , LiftedTRingBuffer
    , SmallTRingBuffer
    , PrimTRingBuffer
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

import GHC.Conc qualified
import Control.Concurrent.STM (STM)
import Control.Concurrent.STM qualified as STM
import Control.Concurrent.STM.TVar (TVar)
import Control.Concurrent.STM.TVar qualified as TVar
import Control.Monad.Primitive (RealWorld)
import Data.Primitive.Contiguous (Contiguous, Element, Mutable, Array, SmallArray, PrimArray)
import Data.Primitive.Contiguous qualified as Contiguous
import System.IO.Unsafe qualified

-- | A Bounded concurrent FIFO queue, built on top of STM (using TVars).
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
data TRingBuffer arr a = TRingBuffer
  { reader :: !(TVar Int)
  , writer :: !(TVar Int)
  , contents :: !(Mutable arr RealWorld a)
  }

type PrimTRingBuffer a = TRingBuffer PrimArray a
type SmallTRingBuffer a = TRingBuffer SmallArray a
type LiftedTRingBuffer a = TRingBuffer Array a

instance (Contiguous arr, Element arr a) => Show (TRingBuffer arr a) where
    show buf = "TRingBuffer {capacity = " <> show (capacity buf) <> ", ...}"

-- | Create a new TRingBuffer
empty :: (Contiguous arr, Element arr a) => Word -> STM (TRingBuffer arr a)
empty cap = do
  reader <- TVar.newTVar 0
  writer <- TVar.newTVar 0
  contents <- GHC.Conc.unsafeIOToSTM $ Contiguous.replicateMut (fromIntegral (cap + 1)) emptyElem
  pure TRingBuffer{reader, writer, contents}

-- | Create a new TRingBuffer, directly in IO (outside of STM)
emptyIO :: (Contiguous arr, Element arr a) => Word -> IO (TRingBuffer arr a)
emptyIO cap = do
  reader <- TVar.newTVarIO 0
  writer <- TVar.newTVarIO 0
  contents <- Contiguous.replicateMut (fromIntegral (cap + 1)) emptyElem
  pure TRingBuffer{reader, writer, contents}

-- | Check the maximum number of elements that can be stored in this TRingBuffer
--
-- Non-blocking. A worst-case O(1) constant-time operation
capacity :: (Contiguous arr, Element arr a) => TRingBuffer arr a -> Int
capacity buf = System.IO.Unsafe.unsafePerformIO $ Contiguous.sizeMut buf.contents

-- | Attempts to add a new element to the TRingBuffer.
--
-- If writing succeeded, returns `True`.
-- If the buffer is full, returns `False`.
--
-- Non-blocking. A worst-case O(1) constant-time operation.
--
-- Calls to `tryPush` are synchronized with any other concurrent calls to
-- `pop`/`push`/`tryPop`/`tryPush`
tryPush :: (Contiguous arr, Element arr a) => TRingBuffer arr a -> a -> STM Bool
tryPush buf a = do
  let !cap = capacity buf
  readIdx <- TVar.readTVar buf.reader
  writeIdx <- TVar.readTVar buf.writer
  let newWriteIdx = (writeIdx + 1) `mod` cap
  if newWriteIdx == readIdx then
      -- Buffer is full
      pure False
  else do
      unsafeWriteElem buf writeIdx a
      TVar.writeTVar buf.writer newWriteIdx
      pure True

-- | Attempts to grab the earliest-written element from the TRingBuffer.
-- 
-- Returns `Nothing` if the buffer is empty.
-- Non-blocking. A worst-case O(1) constant-time operation.
--
-- Calls to `tryPop` are synchronized with any other concurrent calls to
-- `pop`/`push`/`tryPop`/`tryPush`
tryPop :: (Contiguous arr, Element arr a) => TRingBuffer arr a -> STM (Maybe a)
tryPop buf = do
  let !cap = capacity buf
  readIdx <- TVar.readTVar buf.reader
  writeIdx <- TVar.readTVar buf.writer
  if readIdx == writeIdx then
      -- Buffer is empty
      pure Nothing
  else do
      a <- unsafeReadElem buf readIdx
      let newReadIdx = (readIdx + 1) `mod` cap
      TVar.writeTVar buf.reader newReadIdx
      pure (Just a)

-- | Adds a new element to the TRingBuffer.
--
-- If the buffer is full, will block until there is space.
-- (by another thread running `pop` or `tryPop`)
--
-- Calls to `push` are synchronized with any other concurrent calls to
-- `pop`/`push`/`tryPop`/`tryPush`
push :: (Contiguous arr, Element arr a) => TRingBuffer arr a -> a -> STM ()
push buf a = do
    writingSucceeded <- tryPush buf a
    STM.check writingSucceeded

-- | Reads the oldest element from the TRingBuffer.
--
-- If the buffer is empty, will block until an element was written.
-- (by another thread running `push` or `tryPush`)
--
-- Calls to `pop` are synchronized with any other concurrent calls to
-- `pop`/`push`/`tryPop`/`tryPush`
pop :: (Contiguous arr, Element arr a) => TRingBuffer arr a -> STM a
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
emptyElem = (error "attempted to read uninitialized element of TRingBuffer")

-- Read an element of a mutable array in the STM monad.
--
-- Unsafe because no bounds checking is done,
-- and the caller has to ensure that we're not reading an index another thread is writing to.
--
-- Calling this IO op in STM is sound because:
-- - This operation is idempotent
-- - This operation calls a single GHC primop, so we cannot observe a broken state on STM transaction restart
unsafeReadElem :: (Contiguous arr, Element arr a) => TRingBuffer arr a -> Int -> STM a
unsafeReadElem buf idx =
    GHC.Conc.unsafeIOToSTM $ Contiguous.read buf.contents idx

-- Write an element of a mutable array in the STM monad.
--
-- Unsafe because no bounds checking is done,
-- and the caller has to ensure that we're not writing to an index another thread is reading.
--
-- Calling this IO op in STM is sound because:
-- - This operation is idempotent
-- - This operation calls a single GHC primop, so we cannot observe a broken state on STM transaction restart
unsafeWriteElem :: (Contiguous arr, Element arr a) => TRingBuffer arr a -> Int -> a -> STM ()
unsafeWriteElem buf idx a =
    GHC.Conc.unsafeIOToSTM $ Contiguous.write buf.contents idx a
