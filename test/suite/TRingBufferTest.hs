module TRingBufferTest where
import TRingBuffer qualified
import Data.Foldable(for_)
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async.Lifted
import UnliftIO.STM qualified as STM

-- Testing helpers:
import Test.Tasty
import Test.Tasty.Hedgehog
import Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import Control.Monad.IO.Class (liftIO)

test_concurrency :: [TestTree]
test_concurrency = 
    [ testProperty "concurrent pushes and pops lose no data" $ property $ do
        size <- forAll $ Gen.word (Range.linear 1 500)
        elems <- forAll $ Gen.nonEmpty (Range.linear 1 2_000) $ (Gen.enumBounded :: Gen Int)

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
