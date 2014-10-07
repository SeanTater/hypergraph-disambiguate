module Index.Popularity (
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

type Popularity = HM.HashMap Text.Text Int
popularity_threshold = 5

mergeChunks :: [Popularity] -> Popularity
mergeChunks chunks =
    merge $ ((chunkAndMerge chunks) `using` (parList rseq))
    where
        chunkAndMerge [] = []
        chunkAndMerge xs =
            merge front : (chunkAndMerge back)
            where
                (front, back) = splitAt 1000 xs
        merge =
            foldl' (HM.unionWith (+)) HM.empty

countChunk :: [Text.Text] -> Popularity
countChunk tokens =
    HM.fromListWith (+) $ map (\x -> (x, 1)) tokens

trimCount :: Popularity -> Popularity
trimCount counter =
    HM.filter (>popularity_threshold) counter

isPopular :: Popularity -> Text.Text -> Bool
isPopular counter token = HM.lookupDefault 0 token counter > popularity_threshold