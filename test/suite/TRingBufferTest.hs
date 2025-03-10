module TRingBufferTest where
import TRingBuffer (TRingBuffer)
import TRingBuffer qualified
import Data.Foldable(for_)
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted
import UnliftIO.STM (STM)
import UnliftIO.STM qualified as STM

-- Testing helpers:
import Test.Tasty
import Test.Tasty.Hedgehog
import Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import Control.Monad.IO.Class (liftIO)

test_simple :: [TestTree]
test_simple = 
    [ testProperty "tryPush keeps oldest X items" $ property $ do
        size <- forAll $ Gen.word (Range.linear 1 500)
        elems <- forAll $ Gen.list (Range.linear 1 2_000) $ (Gen.enumBounded :: Gen Int)
        buf <- liftIO $ TRingBuffer.emptyIO size

        for_ elems (STM.atomically . TRingBuffer.tryPush buf)
        kept <- STM.atomically (TRingBuffer.popAll buf)
        kept === take (fromIntegral size) elems

    , testProperty "overwritingpush keeps latest X items" $ property $ do
        size <- forAll $ Gen.word (Range.linear 1 500)
        elems <- forAll $ Gen.list (Range.linear 1 2_000) $ (Gen.enumBounded :: Gen Int)
        buf <- liftIO $ TRingBuffer.emptyIO size

        for_ elems (STM.atomically . TRingBuffer.overwritingPush buf)
        kept <- STM.atomically (TRingBuffer.popAll buf)
        kept === reverse (take (fromIntegral size) (reverse elems))

    , testProperty "tryPop returns contents in order, and Nothing when buffer is empty" $ property $ do
        size <- forAll $ Gen.word (Range.linear 1 500)
        elems <- forAll $ Gen.list (Range.linear 1 2_000) $ (Gen.enumBounded :: Gen Int)
        buf <- liftIO $ TRingBuffer.emptyIO size

        for_ elems $ \x -> 
            STM.atomically (TRingBuffer.tryPush buf x)

        let contentLength = min (fromIntegral size) (length elems)
        let expectedContents = take contentLength elems

        for_ expectedContents $ \x -> do
            actual <- STM.atomically (TRingBuffer.tryPop buf) 
            actual === Just x

        final <- STM.atomically (TRingBuffer.tryPop buf) 
        final === Nothing

    ]

test_concurrency :: [TestTree]
test_concurrency = 
    [ testProperty "concurrent pushes and pops lose no data" $ property $ do
        size <- forAll $ Gen.word (Range.linear 1 500)
        elems <- forAll $ Gen.list (Range.linear 1 2_000) $ (Gen.enumBounded :: Gen Int)
        buf <- liftIO $ TRingBuffer.emptyIO size

        let 
            producer = do
                for_ elems $ \x -> do
                    STM.atomically (TRingBuffer.push buf x)
            consumer = do
                race_ (liftIO (threadDelay 1_000_000 >> error "Consumer probably hanging")) $
                    for_ elems $ \x -> do
                        observed <- STM.atomically (TRingBuffer.pop buf)
                        observed === x

        concurrently_ producer consumer
    ]
