module Main where

import Data.Foldable (for_)
import Control.Monad (forever, replicateM)
import Control.Concurrent.Async
import Control.Concurrent.STM qualified as STM
import TRingBuffer (TRingBuffer)
import TRingBuffer qualified


main :: IO ()
main = do
    buffer <- TRingBuffer.emptyIO 7 :: IO (TRingBuffer Integer)
    let producer = for_ [1..] (\a -> STM.atomically (TRingBuffer.push buffer a))
    let consumer = forever ((replicateM 20 (STM.atomically (TRingBuffer.pop buffer))) >>= print)
    let producers = replicateConcurrently 10 producer 
    let consumers = replicateConcurrently 10 consumer

    _ <- concurrently producers consumers
    pure ()
