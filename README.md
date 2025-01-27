
A little example you can run in GHCi:

```haskell
import Data.Foldable (for_)
import Control.Monad (forever, replicateM)
import Control.Concurrent.Async
x <- emptyIO 1000 :: IO (TRingBuffer Int)
let producer = for_ [1..] (\a -> STM.atomically (push x a))
let consumer = forever ((replicateM 10 (STM.atomically (pop x))) >>= print)

concurrently (replicateConcurrently 100 producer) (replicateConcurrently 10 consumer)
```

(Feel free to up the number of consumers as well,
but be aware that their printing will be interleaved)
