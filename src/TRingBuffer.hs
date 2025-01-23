{-# LANGUAGE GHC2021 #-}
module TRingBuffer where

import GHC.Conc qualified
import Control.Concurrent.STM (STM)
import Control.Concurrent.STM qualified as STM
import Control.Concurrent.STM.TVar (TVar)
import Control.Concurrent.STM.TVar qualified as TVar
-- TODO use primitive-unlifted instead maybe?
import Data.Primitive.Array (Array)
import Data.Primitive.Array qualified as Array
import Data.IORef (IORef)
import Data.IORef qualified as IORef

data TRingBuffer a = TRingBuffer
    { readerIdx :: !(TVar Int)
    , writerIdx :: !(TVar Int)
    , contents :: !(IORef (Array (Maybe a)))
    }

new :: Word -> STM (TRingBuffer a)
new capacity = do
    readerIdx <- TVar.newTVar 0
    writerIdx <- TVar.newTVar 0
    contents <- GHC.Conc.unsafeIOToSTM $ IORef.newIORef (initializeContents capacity)
    pure (TRingBuffer readerIdx writerIdx contents)

newIO :: Word -> IO (TRingBuffer a)
newIO capacity = do
    readerIdx <- TVar.newTVarIO 0
    writerIdx <- TVar.newTVarIO 0
    contents <- IORef.newIORef (initializeContents capacity)
    pure (TRingBuffer readerIdx writerIdx contents)

initializeContents :: Word -> Array (Maybe a)
initializeContents capacity = Array.runArray (Array.newArray (fromIntegral (capacity + 1)) Nothing)

tryPut :: TRingBuffer a -> a -> STM Bool
tryPut (TRingBuffer r w c) a = do
    w' <- TVar.readTVar w
    r' <- TVar.readTVar r
    c'<- GHC.Conc.unsafeIOToSTM (IORef.readIORef c)
    let capacity = Array.sizeofArray c'
    if (w' + 1) `mod` capacity == r' then
        -- Buffer is full
        pure False
    else do
        GHC.Conc.unsafeIOToSTM $  do
            -- marr <- Array.thawArray c' 0 capacity
            -- I _think_ but am not 100% sure this is allowed:
            marr <- Array.unsafeThawArray c'
            Array.writeArray marr w' (Just a)
            c'' <- Array.unsafeFreezeArray marr
            IORef.writeIORef c c''
            
        TVar.writeTVar w ((w' + 1) `mod` capacity)
        pure True

tryGet :: TRingBuffer a -> STM (Maybe a)
tryGet (TRingBuffer r w c) = do
    w' <- TVar.readTVar w
    r' <- TVar.readTVar r
    if r' == w' then
        -- Buffer is empty
        pure Nothing
    else do
        c'<- GHC.Conc.unsafeIOToSTM (IORef.readIORef c)
        let capacity = Array.sizeofArray c'
        let a = Array.indexArray c' r'
        TVar.writeTVar r ((r' + 1) `mod` capacity)
        pure a


put :: TRingBuffer a -> a -> STM ()
put buf a = do
    res <- tryPut buf a
    STM.check res

get :: TRingBuffer a -> STM a
get buf = do
    res <- tryGet buf
    case res of
        Nothing -> STM.retry
        Just a -> pure a

-- capacity :: TRingBuffer a -> Int
-- capacity (TRingBuffer _r _w c) = System.IO.Unsafe.unsafePerformIO $ do
--     c' <- IORef.readIORef c
--     pure $ Array.sizeofArray c'
