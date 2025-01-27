module Main where

import Data.Foldable (for_)
import Control.Monad (forever, replicateM)
import Control.Concurrent.Async
import Control.Concurrent.STM qualified as STM
import TRingBuffer (TRingBuffer)
import TRingBuffer qualified
import ARingBuffer (ARingBuffer)
import ARingBuffer qualified as A


main :: IO ()
main = do
    buffer <- TRingBuffer.emptyIO 7 :: IO (TRingBuffer Integer)
    let producer = for_ [1..] (\a -> STM.atomically (TRingBuffer.push buffer a))
    let consumer = forever ((replicateM 13 (STM.atomically (TRingBuffer.pop buffer))) >>= print)
    let producers = replicateConcurrently 3 producer 
    let consumers = replicateConcurrently 1 consumer

    _ <- concurrently producers consumers
    pure ()

-- main :: IO ()
-- main = do
--     x <- A.empty 10000 :: IO (A.ARingBuffer Int)
--     let producer = for_ [1..] (\a -> (A.tryPush x a))
--     -- let consumer = forever ((A.pop x) >>= print)
--     let consumer = forever ((replicateM 1000 ((A.pop x))) >>= print . sum)
--     -- _ <- concurrently producer consumer

--     _ <- concurrently (replicateConcurrently 100 producer) (replicateConcurrently 10 consumer)
--     pure ()
