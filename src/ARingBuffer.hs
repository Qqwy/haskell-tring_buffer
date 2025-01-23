{-# LANGUAGE OverloadedRecordDot #-}
module ARingBuffer where

import GHC.Conc qualified
import Control.Concurrent.STM (STM)
import Control.Concurrent.STM qualified as STM
import Control.Concurrent.STM.TVar (TVar)
import Control.Concurrent.STM.TVar qualified as TVar
-- TODO use primitive-unlifted instead maybe?
import Data.Primitive.Array (Array, MutableArray)
import Data.Primitive.Array qualified as Array
import Control.Monad.Primitive (RealWorld)
-- import Data.IORef (IORef)
-- import Data.IORef qualified as IORef
import Control.Concurrent.MVar (MVar)
import Control.Concurrent.MVar qualified as MVar

data ARingBuffer a = ARingBuffer
  { reader :: !(MVar Int)
  , writer :: !(MVar Int)
  , contents :: !(MutableArray RealWorld a)
  }

empty :: Word -> IO (ARingBuffer a)
empty cap = do
  reader <- MVar.newMVar 0
  writer <- MVar.newMVar 0
  contents <- Array.newArray (fromIntegral (cap + 1)) emptyElem
  pure ARingBuffer{reader, writer, contents}

capacity :: ARingBuffer a -> Int
capacity buf = Array.sizeofMutableArray buf.contents

tryPut :: ARingBuffer a -> a -> IO Bool
tryPut buf a = do
  let !cap = capacity buf
  readIdx <- MVar.readMVar buf.reader
  MVar.modifyMVar buf.writer $ \writeIdx -> do
    let newWriteIdx = (writeIdx + 1) `mod` cap
    if newWriteIdx == readIdx then
      -- Buffer is full
      pure (writeIdx, False)
    else do
      Array.writeArray buf.contents writeIdx a
      pure (newWriteIdx, True)

tryGet :: ARingBuffer a -> IO (Maybe a)
tryGet buf = do
  let !cap = capacity buf
  writeIdx <- MVar.readMVar buf.writer
  MVar.modifyMVar buf.reader $ \readIdx -> do
    if readIdx == writeIdx then
      -- Buffer is empty
      pure (readIdx, Nothing)
    else do
      a <- Array.readArray buf.contents readIdx
      -- Not strictly necessary, but we don't want references
      -- to keep hanging around
      -- (which would delay them being GC'd):
      Array.writeArray buf.contents readIdx emptyElem
      let newReadIdx = (readIdx + 1) `mod` cap
      pure (newReadIdx, Just a)

emptyElem :: a
emptyElem = (error "attempted to read uninitialized element of TRingBuffer")
