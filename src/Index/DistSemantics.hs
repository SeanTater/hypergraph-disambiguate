{-# LANGUAGE BangPatterns, FlexibleInstances #-} -- Data.Binary uses this anyway
module Index.DistSemantics (
    batchGetDistributions,
    Distributions,
    getDistributions,
    serialize,
    stat) where
{-
    You'll need:
        murmur-hash
        vector
-}
import Prelude hiding (mapM_, minimum)
import Data.Int
import Data.Foldable
import Data.Bits ((.&.))
import qualified Data.Bits as Bits
import qualified Data.Binary as Bin
import qualified Data.Binary.Put as Bin
import qualified Data.Binary.Builder as Bin
import qualified Data.Binary.IEEE754 as Bin
import Data.Monoid
import qualified Data.Text as Text
import qualified Data.IntMap as IM
import qualified Data.HashMap.Strict as HM
import qualified Data.Sequence as Seq
import qualified Data.ByteString.Lazy as BS
--import qualified Data.Array.IArray as A
import qualified Data.Vector.Unboxed as V
import Index.Bloom
import Index.Utility

import Data.Group

-- | The sum of a word's sparse-vector "neighborhood"
newtype Context = Context (V.Vector Double) deriving (Show)
emptyContextVector = V.replicate context_dims 0

instance Monoid Context where
    mempty = Context emptyContextVector
    mappend (Context a) (Context b) = Context $ V.zipWith (+) a b
    mconcat = foldl' mappend mempty
instance Group Context where
    invert (Context a) = Context $ V.map (\x -> -x) a

-- | A Dist = frequency count (to subtract this word from it's context) + a Context
data Dist = Dist {-# UNPACK #-} !Int !Context deriving (Show)

instance Monoid Dist where
    mempty = Dist 1 mempty
    mappend (Dist a x) (Dist b y) = Dist (a+b) (mappend x y)
    mconcat = foldl' mappend mempty

-- | Distributions = a mapping from String to Dist
newtype Distributions = Distributions (HM.HashMap Text.Text Dist) deriving (Show)

instance Monoid Distributions where
    mempty = Distributions $ HM.empty
    mappend (Distributions a) (Distributions b) = Distributions $ HM.unionWith (mappend) a b
    mconcat = foldl' mappend mempty



-- Dist doesn't use "instance Binary Dist" because it
-- doesn't satisfy "get.put = id". (The other end is not Haskell so
-- the parsing on the other end is easier.) This setup makes that clearer.
serialize :: Distributions -> [(Text.Text, Int, BS.ByteString)]
serialize (Distributions all_dists) = 
    [ (w, cnt dist, convertToBinary w dist) | (w, dist) <- HM.toList all_dists]
    where
        cnt (Dist x _) = x
        getContextVector (Dist _ (Context v)) = v
        
        -- Serialize just the double[] (not the array length like default)
        convertToBinary :: Text.Text -> Dist -> BS.ByteString
        convertToBinary w dist =
            Bin.toLazyByteString $ Bin.execPut ctx_values
            where
                ctx_values = mapM Bin.putFloat64be $ V.toList $ centerContextVector $ subtractSelfAndUnwrap w dist
        
        
        -- Remove this word's entries from its own context
        subtractSelfAndUnwrap :: Text.Text -> Dist -> V.Vector Double
        subtractSelfAndUnwrap word (Dist count (Context original)) =
            V.zipWith (\x y -> x - (fromIntegral count*y)) original self
            where
                self = getContextVector $ getWordContext word
        
        -- Find the average context vector
        averageContextVector :: V.Vector Double
        averageContextVector = 
            V.map (\x -> x / len) total
            where
                len = fromIntegral $ HM.size all_dists -- this is an O(n) operation otherwise
                total = foldl' (V.zipWith (+)) emptyContextVector $ subtractAssocs $ HM.toList all_dists
                subtractAssocs = map (\(w, d) -> subtractSelfAndUnwrap w d)
        
        -- Subtract out the average context vector
        centerContextVector :: V.Vector Double -> V.Vector Double
        centerContextVector vec =
            V.zipWith (-) vec averageContextVector

-- | n-dimensional space in which words are placed.
-- more dimensions -> less overlap, slower
context_dims = 1024
-- | count of dimensions where a word is not on the axis.
-- more -> more partial collisions, less total collisions
word_dims = 7

-- | Generate the sparse Dist for one word without its neighbors
getWordContext :: Text.Text -> Dist
getWordContext word =
    Dist 1 $ Context $ V.accum (+) emptyContextVector $ map getKV $ getNHashes word_dims word
    where
        getKV hash = 
            (fromIntegral (hash `mod` context_dims), picksign $ Bits.rotate hash 32)
        picksign hash = fromIntegral $ (Bits.popCount hash .&. 1) * 2 - 1

-- | Generate a Distributions map from a tokenized string.
-- The whole string is treated at one logical block of text.
-- So it makes a difference whether you use a list of sentences,
-- a list of paragraphs, or a list of articles. You'll get different
-- results each way. Bloom indicates a word's minimal relevance.
getDistributions :: Bloom -> [Text.Text] -> Distributions
getDistributions bloom block_of_words =
    Distributions $ HM.fromList [(word, merged_block) | word <- block_of_words ]
    where 
        merged_block = mconcat $ map getWordContext $ filter (isPopular bloom) block_of_words
        
-- | Get a merged Distributions from multiple logical blocks of text.
-- Be advised the Distributions may become very large with a large corpus.
batchGetDistributions :: Bloom -> [[Text.Text]] -> Distributions
batchGetDistributions bloom tokenized_chunks =
    mconcat $ map (getDistributions bloom) tokenized_chunks

stat :: Distributions -> String
stat (Distributions cm) = show sz ++ " keys, about " ++ show (sz * 8 / 1024) ++ " MB"
    where
        sz = fromIntegral $ HM.size cm