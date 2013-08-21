module Main where

import Data.Hashable
import Data.Function (on)
import qualified Data.Map as M
import qualified Data.List as L
import qualified Control.Concurrent.Map as CM
import Test.QuickCheck
import Test.QuickCheck.Monadic
import Test.Framework (defaultMain)
import Test.Framework.Providers.QuickCheck2 (testProperty)

-----------------------------------------------------------------------

main = defaultMain [ testProperty "lookup" pLookup
                   ]

-----------------------------------------------------------------------

type Model k v = M.Map k v

eq :: (Eq a, Eq k, Hashable k, Ord k)
   => (Model k v -> a) -> (CM.Map k v -> IO a) -> [(k, v)] -> Property
eq f g xs = monadicIO $ do
    a <- run $ g =<< CM.fromList xs
    let b = f (M.fromList xs)
    assert $ a == b


-----------------------------------------------------------------------

type Key = Int

-----------------------------------------------------------------------

pLookup :: Key -> [(Key,Int)] -> Property
pLookup k = M.lookup k `eq` CM.lookup k

