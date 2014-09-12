module Index.Bloom (
    countChunk,
    isPopular,
    Bloom(..)
    ) where
import Data.Monoid
import Data.Word
import Control.Monad.Primitive
import qualified Data.Text as Text
import qualified Data.Vector.Unboxed as V
import qualified Data.Vector.Unboxed.Mutable as VM
import Control.Monad.ST
import Index.Utility
-- -- Bloom
    
{-
 - Simple counting bloom filter
 - It will add a word to a filter until it reaches the bloom_threshold
 - Then it will no longer add (to avoid overflow) but will return popular=True
 -}
 
-- | Designed for a bloom of 450k elements
bloom_hash_count = 7
bloom_bin_count = 8388608
bloom_threshold = 50 :: Word8

-- | Monadic Bloom container
type MutableBloom m = VM.MVector (PrimState m) Word8
-- | Bloom container
newtype Bloom = Bloom (V.Vector Word8)

instance Monoid Bloom where
    mempty = Bloom $ V.replicate bloom_bin_count 0
    mappend (Bloom x) (Bloom y) =
        Bloom $ V.zipWith addClamp x y
        where
            -- If a or b > 1/2 the range they are above the threshold anyway
            -- So the loss of precision here is not really a big deal
            addClamp a b = max a $ max b $ a+b

-- | Add a token to a monadic bloom filter
-- and return whether the token has passed the popularity threshold
addToBloom :: (PrimMonad m) => MutableBloom m -> Text.Text -> m ()
addToBloom bloom word = do
    counts <- sequence [ VM.read bloom i | i <- indices ]
    mapM_ (applyMV (incrementClamp) bloom) indices
    where
        incrementClamp _ a = min 255 $ max a (a+1)
        indices = [ hash `mod` bloom_bin_count | hash <- getNHashes bloom_hash_count word ]

isPopular :: Bloom -> Text.Text -> Bool
isPopular (Bloom bloom) word = do
    (minimum [(V.!) bloom i | i <- indices ]) >= bloom_threshold
    where
        indices = [ hash `mod` bloom_bin_count | hash <- getNHashes bloom_hash_count word ]


countChunk :: [Text.Text] -> Bloom
countChunk paragraphs = runST $ do
    workbloom <- VM.replicate bloom_bin_count (0::Word8)
    mapM_ (addToBloom workbloom) paragraphs
    endbloom <- V.freeze workbloom
    return $ Bloom endbloom