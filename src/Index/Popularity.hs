module Index.Popularity (
    appendChunk,
    countChunk,
    isPopular,
    trimCount,
    Popularity,
    mergeChunks
    ) where
import Prelude hiding (foldl, foldr, foldl', concat)
import Data.Monoid
import Control.Parallel.Strategies
import Control.Parallel
import qualified Data.Text as Text
import qualified Data.Vector.Unboxed as V
import qualified Data.Vector.Unboxed.Mutable as VM
import qualified Data.HashMap.Strict as HM
import Data.Foldable
import Control.Monad.ST
import Index.Utility
import qualified Data.Stream as St

type Popularity = HM.HashMap Text.Text Int
popularity_threshold = 10

mergeChunks :: [Popularity] -> Popularity
mergeChunks chunks =
    foldt (HM.unionWith (+)) HM.empty chunks

appendChunk :: Popularity -> Popularity -> Popularity
appendChunk = HM.unionWith (+)

countChunk :: [Text.Text] -> Popularity
countChunk tokens =
    HM.fromListWith (+) $ map (\x -> (x, 1)) tokens

trimCount :: Popularity -> Popularity
trimCount = HM.filter (>popularity_threshold)

isPopular :: Popularity -> Text.Text -> Bool
isPopular counter token = HM.lookupDefault 0 token counter > popularity_threshold

-- The following folds come from the Haskell Wiki
foldt            :: (a -> a -> a) -> a -> [a] -> a
foldt f z []     = z
foldt f z [x]    = x
foldt f z xs     = foldt f z (pairs f xs)
 
foldi            :: (a -> a -> a) -> a -> [a] -> a
foldi f z []     = z
foldi f z (x:xs) = f x (foldi f z (pairs f xs))
 
pairs            :: (a -> a -> a) -> [a] -> [a]
pairs f (x:y:t)  = f x y `par` (f x y : pairs f t)
pairs f t        = t