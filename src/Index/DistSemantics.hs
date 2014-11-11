{-# LANGUAGE BangPatterns, FlexibleInstances, OverloadedStrings #-} -- Data.Binary uses this anyway
module Index.DistSemantics (
    batchMkDistributions,
    cosineSimilarity,
    Distributions,
    getNeighbors,
    mkDistributions,
    lookupWord,
    mkWord,
    serialize,
    stat) where
{-
    You'll need:
        murmur-hash
        vector
-}
import Prelude hiding (mapM_, minimum, sum, concat, foldl, foldl', foldr)
import Data.Int
import Data.Foldable
import Data.Bits ((.&.))
import qualified Data.Bits as Bits
import qualified Data.Binary as Bin
import qualified Data.Binary.Put as Bin
import qualified Data.Binary.Builder as Bin
import qualified Data.Binary.IEEE754 as Bin
import Data.Monoid
import Data.Group
import qualified Data.Text as Text
import qualified Data.IntMap as IM
import qualified Data.HashMap.Strict as HM
import qualified Data.Sequence as Seq
import qualified Data.ByteString.Lazy as BS
import qualified Data.Vector.Unboxed as V
import Index.Utility
import Control.Parallel.Strategies
import Control.Parallel
import Data.Word
import qualified Data.List.Stream as St
import Debug.Trace
import Control.DeepSeq

-- | The sum of a word's sparse-vector "neighborhood"
newtype Context = Context (V.Vector Double)
    deriving (Show)
emptyContextVector = V.replicate context_dims (0::Double)

instance Monoid Context where
    mempty = Context emptyContextVector
    mappend (Context a) (Context b) = Context $ V.zipWith (+) a b
    mconcat l = foldr mappend mempty l
instance Group Context where
    invert (Context a) = Context $ V.map (\x -> -x) a

-- | A Dist = frequency count (to subtract this word from it's context) + a Context
data DSWord = DSWord { dscount :: !Int
                     , dstext :: !Text.Text
                     , dscontext :: !Context
                     , dsstream :: [(Int, Double)]
                     } deriving (Show)

instance Monoid DSWord where
    mempty = DSWord { dscount = 1
                    , dscontext = mempty
                    , dsstream = []
                    , dstext = ""
                    }
    mappend a b = DSWord { dscount = dscount a + dscount b
                    , dscontext = Context $ V.zipWith (+) (merged a) (merged b)
                    , dsstream = []
                    , dstext = dstext (if Text.null $ dstext a
                                        then b
                                        else a)
                    }
        where
            merged x = V.accum (+) v (dsstream x)
                where (Context v) = dscontext x
    
    mconcat xs = DSWord { dscount = sum $ fmap dscount xs
                    , dscontext = mconcat (fmap dscontext xs)
                    , dsstream = []
                    , dstext = dstext $ head xs
                    }
        where
            merged_streams = V.accum (+) emptyContextVector $ St.concatMap dsstream xs
--    mconcat x = foldl' mappend mempty x
        

-- | Distributions = a mapping from String to Dist
newtype Distributions = Distributions (HM.HashMap Text.Text DSWord) deriving (Show)

instance NFData Distributions where
    rnf a = a `seq` ()

instance Monoid Distributions where
    mempty = Distributions HM.empty
    mappend (Distributions a) (Distributions b) = Distributions $ HM.unionWith mappend a b
    mconcat chunks =
        merge (chunkAndMerge chunks `using` parList rseq)
        where
            chunkAndMerge [] = []
            chunkAndMerge xs =
                merge front : chunkAndMerge back
                where
                    (front, back) = splitAt 1000 xs
            merge =
                foldl' mappend mempty


-- Dist doesn't use "instance Binary Dist" because it
-- doesn't satisfy "get.put = id". (The other end is not Haskell so
-- the parsing on the other end is easier.) This setup makes that clearer.
serialize :: Distributions -> [(Text.Text, Int, BS.ByteString)]
serialize (Distributions all_dists) = 
    [ (dstext word, dscount word, convertToBinary word) | word <- HM.elems all_dists]
    where
        
        -- Serialize just the double[] (not the array length like default)
        convertToBinary :: DSWord -> BS.ByteString
        convertToBinary word =
            Bin.toLazyByteString $ Bin.execPut ctx_values
            where
                ctx_values = mapM Bin.putFloat64be $ V.toList $ centerContextVector $ getNeighbors word
        
        -- Find the average context vector
        averageContextVector :: V.Vector Double
        averageContextVector = 
            V.map (/ len) total
            where
                len = fromIntegral $ HM.size all_dists -- this is an O(n) operation otherwise
                total = foldl' (V.zipWith (+)) emptyContextVector $ subtractEach $ HM.elems all_dists
                subtractEach = map getNeighbors
        
        -- Subtract out the average context vector
        centerContextVector :: V.Vector Double -> V.Vector Double
        centerContextVector vec =
            V.zipWith (-) vec averageContextVector

cosineSimilarity :: V.Vector Double -> V.Vector Double -> Double
cosineSimilarity first second =
    (sum elemwise_product)
    /
    ((sqrt $ sum $ elemwise_square first) * (sqrt $ sum $ elemwise_square second))
    where
        l = filter (\x -> abs x > 0.01) . V.toList
        elemwise_product = zipWith (\x y -> x*y) (l first) (l second)
        elemwise_square vec = map (\a -> a * a) (l vec)

-- | n-dimensional space in which words are placed.
-- more dimensions -> less overlap, slower
context_dims = 256
-- | count of dimensions where a word is not on the axis.
-- more -> more partial collisions, less total collisions
word_dims = 2

getNeighbors :: DSWord -> V.Vector Double
getNeighbors word =
    V.zipWith (\x y -> x - (fromIntegral count*y)) original_vec self_vec
    where
        count = dscount word
        (Context original_vec) = dscontext word
            -- Get _just_ this one word
        (Context self_vec) = dscontext $ mkWord (dstext word)

-- | Extract a DSWord given its text
lookupWord :: Text.Text -> Distributions -> Maybe DSWord
lookupWord name (Distributions dists) =
    HM.lookup name dists

-- | Generate the sparse Dist for one word without its neighbors
mkWord :: Text.Text -> DSWord
mkWord text =
    DSWord { dscount = 1
           , dscontext = Context emptyContextVector
           , dsstream = fmap getKV $ getNHashes word_dims text
           , dstext = text
    }
    where
        getKV :: Word64 -> (Int, Double)
        getKV hash = 
            (fromIntegral (hash `rem` fromIntegral context_dims), fromIntegral sign)
            where sign = (Bits.popCount hash .&. 1) * 2 - 1

-- | Generate a Distributions map from a tokenized string.
-- The whole string is treated at one logical block of text.
-- So it makes a difference whether you use a list of sentences,
-- a list of paragraphs, or a list of articles. You'll get different
-- results each way. Bloom indicates a word's minimal relevance.
mkDistributions :: [Text.Text] -> Distributions
mkDistributions block_of_tokens =
    -- the reason for setting dstext= here is because mconcat doesn't know
    -- which word we want to attach it to
    Distributions $ HM.fromList [(token, merged_block {dstext=token, dscount=word_count token}) | token <- block_of_tokens ]
    where 
        word_count token = length $ filter (==token) block_of_tokens
        merged_block = mconcat $ map mkWord block_of_tokens
        
-- | Get a merged Distributions from multiple logical blocks of text.
-- Be advised the Distributions may become very large with a large corpus.
batchMkDistributions :: [[Text.Text]] -> Distributions
batchMkDistributions tokenized_chunks =
    mconcat $ map mkDistributions tokenized_chunks

stat :: Distributions -> String
stat (Distributions cm) = show sz ++ " keys, about " ++ show (sz * 8 / 1024) ++ " MB"
    where
        sz = fromIntegral $ HM.size cm