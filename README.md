
A little example you can run in GHCi:

```haskell
import Data.Foldable (for_)
import Control.Monad (forever)
import Control.Concurrent.Async
x <- emptyIO 1 :: IO (TRingBuffer Int)
let producer = for_ [1..] (\a -> STM.atomically (push x a))
let consumer = forever (STM.atomically (pop x) >>= print)

concurrently (replicateConcurrently 100 producer) (replicateConcurrently 1 consumer)
```

(Feel free to up the number of consumers as well,
but be aware that their printing will be interleaved)
