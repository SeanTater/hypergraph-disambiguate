{-# LANGUAGE BangPatterns, FlexibleInstances #-} -- Data.Binary uses this anyway
module Index.RandomIndexing (
    stat,
    Distributions(..),
    addBinaryMultiwordContext) where
{-
    You'll need:
        murmur-hash
        vector
-}
import Prelude hiding (mapM_, minimum)
import Data.Int
import Data.Foldable
import Data.Bits (popCount, (.&.))
import Data.Binary
import Data.Monoid
import qualified Data.Text as Text
import qualified Data.IntMap as IM
import qualified Data.HashMap.Strict as M
import qualified Data.Sequence as Seq
import Index.Bloom
import Index.Utility

import Data.Group

-- | The sum of a word's sparse-vector "neighborhood"
newtype Context = Context (IM.IntMap Double) deriving (Show)

instance Monoid Context where
    mempty = Context $ IM.empty
    mappend (Context a) (Context b) = Context $ IM.unionWith (+) a b
    mconcat = foldl' mappend mempty
instance Group Context where
    invert (Context a) = Context $ IM.map (\x -> -x) a

-- | A Dist = frequency count (to subtract this word from it's context) + a Context
data Dist = Dist {-# UNPACK #-} !Int !Context deriving (Show)

instance Monoid Dist where
    mempty = Dist 1 mempty
    mappend (Dist a x) (Dist b y) = Dist (a+b) (mappend x y)
    mconcat = foldl' mappend mempty
    
-- Dist doesn't use "instance Binary Dist" because it
-- doesn't satisfy "get.put = id". (The other end is not Haskell so
-- the parsing on the other end is easier.) This setup makes that clearer.
serialize :: Distributions -> [(Text.Text, BS.ByteString)]
serialize dist = 
    [ (w, serializeDist dist) | (w, dist) <- HM.toList]
    where
        serializeDist :: Dist -> Put
        serializeDist (Dist cnt ctx) =
            mapM put ctx_values
            where
                ctx_values = [IM.findWithDefault 0 i ctx | i <- [0..context_dims-1]]

-- | Distributions = a mapping from String to Dist
newtype Distributions = Distributions (HM.HashMap Text.Text Dist) deriving (Show)

instance Monoid Distributions where
    mempty = Distributions $ HM.empty
    mappend (Distributions a) (Distributions b) = Distributions $ HM.unionWith (mappend) a b
    mconcat = foldl' mappend mempty

-- | n-dimensional space in which words are placed.
-- more dimensions -> less overlap, slower
context_dims = 1024
-- | count of dimensions where a word is not on the axis.
-- more -> more partial collisions, less total collisions
word_dims = 7

-- | Generate the sparse Dist for one word without its neighbors
getWordContext :: Text.Text -> Dist
getWordContext word =
    Dist 1 $ Context $ IM.fromList $ map getKV $ getNHashes word_dims word
    where
        getKV hash = 
            (fromIntegral (hash `mod` context_dims), picksign $ rotate hash 32)
        picksign hash = fromIntegral $ (popCount hash .&. 1) * 2 - 1

-- | Generate a Distributions map from a tokenized string.
-- The whole string is treated at one logical block of text.
-- So it makes a difference whether you use a list of sentences,
-- a list of paragraphs, or a list of articles. You'll get different
-- results each way. Bloom indicates a word's minimal relevance.
getDistributions :: Bloom -> [Text.Text] -> Distributions
getDistributions block_of_words =
    Distributions $ HM.fromList [(word, merged_block) | word <- block_of_words ]
    where 
        merged_block = mconcat $ map getWordContext $ filter (isPopular bloom) block_of_words
        
-- | Get a merged Distributions from multiple logical blocks of text.
-- Be advised the Distributions may become very large with a large corpus.
batchGetDistributions :: Bloom -> Distributions -> (Seq.Seq [Text.Text]) -> Distributions
indexBinaryChunks bloom tokenized_chunks =
    foldl' (getDistributions bloom) mempty tokenized_chunks

stat :: Distributions -> String
stat (Distributions cm) =
    "Context Map Statistics: "
        ++ show sz ++ " keys, about" ++ show (sz * 8 / 1024) ++ " MB"
    where
        sz = fromIntegral $ M.size cm