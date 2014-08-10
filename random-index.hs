{-
    You'll need:
        murmur-hash
        vector
-}
{-# LANGUAGE DataKinds #-}

import GHC.TypeLits
import qualified Data.Vector.Unboxed as V

--import Data.Bits
import Data.Word
import Data.Int
import qualified Data.Map.Strict as M
import Data.List -- mapAccumL
import qualified Data.Digest.Murmur64 as Murmur



-- -- Hash

-- Feel free to set these
-- context_dims: n-dimensional space in which words are placed
--               more dimensions -> less overlap, less performance
context_dims = 1024 :: Int
-- word_dims: count of dimensions where a word is not on the axis
--            more -> more partial collisions, less total collisions
word_dims = 20
-- Balance range with memory using by changing this type
context_type = 0 :: Int64

{- Get the hash of a word (or any String)
 -
 - This vector is derived from repeated Murmur hashes
 - this vector represents the word when it is added to a context vector
 -}
 
getNHashes n word =
    [h seed | seed <- [1..n]]
    where h seed = fromIntegral $ Murmur.asWord64 $ Murmur.hash64WithSeed seed word
          
hashWord word =
    V.generate context_dims (\i -> maybe 0 (\x -> x) (lookup i indices))
    where
        hashes = getNHashes word_dims word
        evenOddToPosNeg x = (mod x 2) * 2 - 1
        hashToIndex hash = hash `mod` context_dims
        indices = [ (hashToIndex hash, evenOddToPosNeg $ fromIntegral hash) | hash <- hashes ]

-- Join hash-vectors of the words in the paragraph
hashChunk tokens =
    foldl' hashAnotherWord newEmptyHash tokens
    where
        hashAnotherWord vec word = V.zipWith (+) vec (hashWord word)

newEmptyHash = V.replicate context_dims context_type



-- -- Bloom
    
{-
 - Simple counting bloom filter
 - It will add a word to a filter until it reaches the bloom_threshold
 - Then it will no longer add (to avoid overflow) but will return popular=True
 -}
bloom_hash_count = 10
bloom_bin_count = 25000000
bloom_threshold = 0

-- Count a word, Return (is_popular, new_bloom)
addToBloom :: V.Vector Word8 -> String -> (Bool, V.Vector Word8)
addToBloom bloom word =
    (popular, V.accum (\a b -> max a (a+b)) bloom [ (idx, 1) | idx <- indices, not popular])
    where
        indices = [ hash `mod` bloom_bin_count | hash <- getNHashes bloom_hash_count word ]
        min_count = minimum [ (V.!) bloom idx | idx <- indices ]
        popular = min_count >= bloom_threshold
        
makeNewBloom = 
    V.replicate bloom_bin_count (0::Word8)

-- -- Bloomed Map
data BloomMap = BloomMap (V.Vector Word8, M.Map String (V.Vector Int64) ) deriving (Show)

    
    
    
-- -- Main

-- If the word appears in the bloom at least n times, add context vectors
addWordContext :: ((V.Vector Word8, M.Map String (V.Vector Int64)), V.Vector Int64) -> String -> ((V.Vector Word8, M.Map String (V.Vector Int64)), V.Vector Int64)
addWordContext ((bloom, context_map), new_context) word
    | popular = ((new_bloom, new_context_map), new_context)
    | otherwise = ((new_bloom, context_map), new_context)
    where
        (popular, new_bloom) = addToBloom bloom word
        new_context_map = M.insertWith (V.zipWith (+)) word new_context context_map

-- Chain addWordContext, but only calculate paragraph context once (used by binaryChunkContext)
addBinaryMultiwordContext (bloom, context_map) tokens =
    new_bloom_map
    where
        chunk_context = hashChunk tokens
        (new_bloom_map, _) = foldl' (addWordContext) ((bloom, context_map), chunk_context) tokens

-- Fill contexts based on whether words cooccur in a chunk
indexBinaryChunks tokenized_chunks =
    foldl' addBinaryMultiwordContext (makeNewBloom, M.empty) tokenized_chunks
        
    

main = do
    line <- getLine
    print $ indexBinaryChunks $ [ words line ]