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
    -- * Blocking (re-trying) interface
    , push
    , pop
    -- * Non-blocking (retry-less) interface
    , tryPush
    , tryPop
    , popAll
    -- * Overwrite oldest element
    , overwritingPush
    -- * Reading buffer contents without removing elements
    , tryPeek
    , peekAll
)
where

import Control.Concurrent.STM (STM)
import Control.Concurrent.STM qualified as STM
import Control.Concurrent.STM.TVar (TVar)
import Control.Concurrent.STM.TVar qualified as TVar
import Control.Concurrent.STM.TArray (TArray)
import Data.Array.Base (MArray)
import Data.Ix (Ix)
import Data.Array.Base qualified as Array
import System.IO.Unsafe qualified

-- $setup
-- >>> import TRingBuffer qualified
-- >>> import Control.Concurrent.STM qualified as STM

-- | A Bounded Concurrent STM-based FIFO Queue, implemented as a Ring Buffer
--
-- Goals of this datastructure:
-- - Easy to use; no hidden footguns
-- - Predictable memory usage
-- - Assuming a single reader and writer, O(1) pushing and O(1) popping.
-- - Readers do not usually conflict with writers (only once every `capacity / 2` operations)
-- - Writers do not usually conflict with readers (only once every `capacity / 2` operations)
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

-- | This instance is useful for debugging and introspection
-- but do not use it inside STM
-- as it uses STM + `peekAll` under the hood.
--
--
-- >>> buf <- STM.atomically $ TRingBuffer.empty 10 :: IO (TRingBuffer Int)
-- >>> mapM_ (\x -> STM.atomically (tryPush buf x)) [0..10]
-- >>> buf
-- TRingBuffer {capacity = 10, contents = [0,1,2,3,4,5,6,7,8,9] }
-- >>> STM.atomically (TRingBuffer.peekAll buf)
-- [0,1,2,3,4,5,6,7,8,9]
-- >>> buf
-- TRingBuffer {capacity = 10, contents = [0,1,2,3,4,5,6,7,8,9] }
instance Show a => Show (TRingBuffer a) where
    show buf = "TRingBuffer {capacity = " <> show (capacity buf) <> ", contents = " <> elems <> " }"
      where
        elems = show $ System.IO.Unsafe.unsafePerformIO $ STM.atomically (peekAll buf)

-- | Create a new TRingBuffer with the given max `capacity`
--
-- >>> STM.atomically $ TRingBuffer.empty 4
-- TRingBuffer {capacity = 4, contents = [] }
empty :: Word -> STM (TRingBuffer a)
{-# INLINABLE empty #-}
empty cap = do
  let readIdx = 0
  let writeIdx = 0
  readerAndCachedWriter <- TVar.newTVar (Indexes readIdx writeIdx)
  writerAndCachedReader <- TVar.newTVar (Indexes writeIdx readIdx)
  contents <- emptyContents cap
  pure TRingBuffer{readerAndCachedWriter, writerAndCachedReader, contents}

-- | Create a new TRingBuffer, directly in IO (outside of STM)
--
-- >>> TRingBuffer.emptyIO 100
-- TRingBuffer {capacity = 100, contents = [] }
emptyIO :: Word -> IO (TRingBuffer a)
{-# INLINABLE emptyIO #-}
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
--
-- >>> buf <- TRingBuffer.emptyIO 100
-- >>> TRingBuffer.capacity buf
-- 100
capacity :: TRingBuffer a -> Word
{-# INLINABLE capacity #-}
capacity buf = 
    -- SAFETY: Once the array is allocated,
    -- we never grow or shrink it,
    -- so this call is outwardly pure
    let cap = System.IO.Unsafe.unsafePerformIO $ capacity' buf
    in -- See NOTE [capacity]
      cap - 1

-- Returns the _true_ capacity, including the extra element
capacity' :: MArray TArray a m => TRingBuffer a -> m Word
{-# INLINABLE capacity' #-}
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
-- one every `capacity / 2` pushes will synchronize with a pop.
--
--
-- >>> buf <- STM.atomically $ TRingBuffer.empty 3 :: IO (TRingBuffer Int)
-- >>> buf
-- TRingBuffer {capacity = 3, contents = [] }
-- >>> STM.atomically $ TRingBuffer.tryPush buf 1
-- True
-- >>> buf
-- TRingBuffer {capacity = 3, contents = [1] }
-- >>> STM.atomically $ TRingBuffer.tryPush buf 2
-- True
-- >>> buf
-- TRingBuffer {capacity = 3, contents = [1,2] }
-- >>> STM.atomically $ TRingBuffer.tryPush buf 3
-- True
-- >>> buf
-- TRingBuffer {capacity = 3, contents = [1,2,3] }
-- >>> STM.atomically $ TRingBuffer.tryPush buf 4
-- False
-- >>> buf
-- TRingBuffer {capacity = 3, contents = [1,2,3] }
tryPush :: TRingBuffer a -> a -> STM Bool
{-# INLINABLE tryPush #-}
tryPush buf a = do
  !cap <- capacity' buf
  Indexes writeIdx readIdxCached <- TVar.readTVar buf.writerAndCachedReader
  let newWriteIdx = modularInc writeIdx cap
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

-- | Attempts to read-and-remove the earliest-written element from the TRingBuffer.
-- 
-- Returns `Nothing` if the buffer is empty.
--
-- Non-blocking. A worst-case O(1) constant-time operation.
--
-- Calls to `tryPop` are synchronized with any other concurrent calls to
-- `pop`/`tryPop`/`peek` (using `STM.retry`)
--
-- Most of the time, popping can happen concurrently with pushing.
-- Assuming pushes and pops happen at the same rate (on average),
-- one every `capacity / 2` pops will synchronize with a push.
--
-- 
-- >>> buf <- STM.atomically $ TRingBuffer.empty 3 :: IO (TRingBuffer Int)
-- >>> mapM_ (\x -> STM.atomically (tryPush buf x)) [1,2,3]
-- >>> buf
-- TRingBuffer {capacity = 3, contents = [1,2,3] }
--
-- >>> STM.atomically (tryPop buf)
-- Just 1
-- >>> buf
-- TRingBuffer {capacity = 3, contents = [2,3] }
-- >>> STM.atomically (tryPop buf)
-- Just 2
-- >>> buf
-- TRingBuffer {capacity = 3, contents = [3] }
-- >>> STM.atomically (tryPop buf)
-- Just 3
-- >>> buf
-- TRingBuffer {capacity = 3, contents = [] }
-- >>> STM.atomically (tryPop buf)
-- Nothing
-- >>> buf
-- TRingBuffer {capacity = 3, contents = [] }
tryPop :: TRingBuffer a -> STM (Maybe a)
{-# INLINABLE tryPop #-}
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
      let newReadIdx = modularInc readIdx cap
      TVar.writeTVar buf.readerAndCachedWriter (Indexes newReadIdx writeIdx)
      pure (Just a)

-- | Attempts to read the earliest-written element from the TRingBuffer, without removing it
--
-- Returns `Nothing` if the buffer is empty.
--
-- Non-blocking. A worst-case O(1) constant-time operation.
--
-- Calls to `tryPeek` are synchronized with any other concurrent calls to `pop`/`tryPop`
tryPeek :: TRingBuffer a -> STM (Maybe a)
{-# INLINABLE tryPeek #-}
tryPeek buf = do
  Indexes readIdx writeIdxCached <- TVar.readTVar buf.readerAndCachedWriter
  if readIdx == writeIdxCached then do
    -- Buffer may be empty, double-check with real write index
    Indexes writeIdxFresh _ <- TVar.readTVar buf.writerAndCachedReader
    if readIdx == writeIdxFresh then do
        -- Buffer is truly empty, nothing more to do
      pure Nothing
    else do
      -- Buffer not empty after all.
      -- Peek
      -- and also update the cached write idx
      TVar.writeTVar buf.readerAndCachedWriter (Indexes readIdx writeIdxFresh)
      actuallyPeek readIdx
  else
    actuallyPeek readIdx
  where
    actuallyPeek readIdx = do
      a <- Array.readArray buf.contents readIdx
      pure (Just a)

-- | Reads the current contents of the buffer without removing any elements
--
-- This function is useful for debugging and introspection
-- but it is not recommended to use in production situations;
-- it will synchronize with any ongoing pushes/pops.
peekAll :: TRingBuffer a -> STM ([a])
{-# INLINABLE peekAll #-}
peekAll buf = do
  Indexes readIdx _ <- TVar.readTVar buf.readerAndCachedWriter
  Indexes writeIdx _ <- TVar.readTVar buf.writerAndCachedReader
  !cap <- capacity' buf
  peekAll' cap readIdx writeIdx
  where
    peekAll' cap readIdx writeIdx = 
      if readIdx == writeIdx then pure []
      else do
        a <- Array.readArray buf.contents readIdx
        !as <- peekAll' cap (modularInc readIdx cap) writeIdx
        pure (a : as)



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
-- one every `capacity / 2` pushes will synchronize with a pop.
push :: TRingBuffer a -> a -> STM ()
{-# INLINABLE push #-}
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
-- one every `capacity / 2` pops will synchronize with a push.
pop :: TRingBuffer a -> STM a
{-# INLINABLE pop #-}
pop buf = do 
    res <- tryPop buf
    case res of
        Nothing -> STM.retry
        Just a -> pure a

-- | Similar to `push`, but instead of, when full, block-and-waiting for a `pop` to make space,
-- the oldest value is immediately overwritten instead.
--
-- So e.g. in a buffer with capacity `4`,
-- if it currently contains `[0,1,2,3]`
-- and we `overwritingPush 42` into it,
-- the `0` will be forgotten, and buffer will be updated to `[1,2,3,42]`
--
-- This function always synchronizes with other pushing operations.
-- It will synchronize with popping operations iff the buffer is currently full.
--
-- >>> buf <- STM.atomically $ TRingBuffer.empty 3 :: IO (TRingBuffer Int)
-- >>> mapM_ (\x -> STM.atomically (tryPush buf x)) [1,2,3]
-- >>> buf
-- TRingBuffer {capacity = 3, contents = [1,2,3] }
-- >>> STM.atomically $ TRingBuffer.overwritingPush buf 4
-- >>> buf
-- TRingBuffer {capacity = 3, contents = [2,3,4] }
-- >>> STM.atomically $ TRingBuffer.overwritingPush buf 100
-- >>> buf
-- TRingBuffer {capacity = 3, contents = [3,4,100] }
overwritingPush :: TRingBuffer a -> a -> STM ()
overwritingPush buf a = do
  !cap <- capacity' buf
  Indexes writeIdx readIdxCached <- TVar.readTVar buf.writerAndCachedReader
  let newWriteIdx = modularInc writeIdx cap
  if newWriteIdx == readIdxCached then do
    -- Buffer may be full, double-check with real read index
    Indexes readIdxFresh _ <- TVar.readTVar buf.readerAndCachedWriter
    if newWriteIdx == readIdxFresh then do
      -- Buffer truly full.
      -- In this case we block with both readers and writers,
      -- overwrite the oldest value
      -- and bump both indexes by one slot
      let newReadIdx = modularInc readIdxFresh cap
      actuallyPush writeIdx newWriteIdx newReadIdx
      TVar.writeTVar buf.readerAndCachedWriter (Indexes newReadIdx newWriteIdx)
    else
      -- Buffer not full after all.
      -- Save value, update write idx 
      -- and also update cached read idx
      actuallyPush writeIdx newWriteIdx readIdxFresh
  else
    -- There is space, so we can do a normal push
    -- and don't need to block any readers
    actuallyPush writeIdx newWriteIdx readIdxCached
  where
    actuallyPush writeIdx newWriteIdx readIdx = do
      Array.writeArray buf.contents writeIdx a
      TVar.writeTVar buf.writerAndCachedReader (Indexes newWriteIdx readIdx)

-- | Repeatedly calls `tryPop` until the buffer is empty
--
-- This function is mainly useful for testing, or for draining
-- remaining items during e.g. shutdown cleanup
--
--
-- >>> buf <- STM.atomically $ TRingBuffer.empty 10 :: IO (TRingBuffer Int)
-- >>> mapM_ (\x -> STM.atomically (tryPush buf x)) [0..10]
-- >>> buf
-- TRingBuffer {capacity = 10, contents = [0,1,2,3,4,5,6,7,8,9] }
-- >>> STM.atomically (TRingBuffer.popAll buf)
-- [0,1,2,3,4,5,6,7,8,9]
-- >>> buf
-- TRingBuffer {capacity = 10, contents = [] }
-- >>> STM.atomically (TRingBuffer.popAll buf)
-- []
popAll :: TRingBuffer a -> STM [a]
popAll buf = do
  mx <- tryPop buf
  case mx of
    Nothing -> pure []
    Just x -> do
      xs <- popAll buf
      pure (x : xs)

-- Used a single shared 'default' element
-- that is used for empty spots in the array.
--
-- This reduces a level of Pointer indirection vs storing `Maybe a` inside the array
emptyElem :: a
emptyElem = (error "attempted to read an uninitialized element of a TRingBuffer. This should be impossible, and thus indicates a bug in TRingBuffer.")

-- Incrementing with an equality check
-- results in more efficient assembly
-- than using (x + 1 `mod` cap)
modularInc :: Word -> Word -> Word
modularInc x cap = if succ x == cap then 0 else succ x
