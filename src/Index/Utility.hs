{-# LANGUAGE BangPatterns #-}
module Index.Utility where
import qualified Data.Digest.Murmur64 as Murmur
import qualified Data.Text.Encoding as TextEncoding
import qualified Data.Vector.Unboxed as V
import qualified Data.Vector.Unboxed.Mutable as VM
import qualified Data.HashSet as HS
import qualified Data.Text as Text
import Data.Foldable (foldl')
import Control.Parallel
import Control.DeepSeq
import Debug.Trace
--import qualified Data.List.Stream as LS
import Control.Monad.Primitive
import Control.Concurrent
import System.IO.Unsafe
import Data.Word

{-| Get murmur-based hashes of a string.
Multiple hashes are made by replacing the seed value with
integers from 1 to n.
-}
getNHashes :: Word64 -> Text.Text -> [Word64]
getNHashes n word =
    [h seed | seed <- [1..n]]
    where h seed = Murmur.asWord64 $ Murmur.hash64WithSeed seed $ TextEncoding.encodeUtf8 word

-- -- Utility
-- | Apply a pure function (with index) to a list of indices on a monadic vector.
applyMV :: (PrimMonad m, V.Unbox t) => (Int -> t -> t) -> VM.MVector (PrimState m) t -> Int -> m ()
applyMV func vec i = do
    e_in <- VM.read vec i
    VM.write vec i (func i e_in)
    return ()
    

-- | Flatten a merge stack
flatten :: (Int -> a -> a -> a) -- ^ Reduction operator
    -> a                        -- ^ Default
    -> [(Int, a)]               -- ^ Stack
    -> a
flatten binop def = snd . foldl' (\(i, x) (j, y) -> (i+j, binop (i+j) y x)) (1, def)

-- | Push stuff onto a merge stack
push :: (NFData a)
    => (Int -> a -> a -> a)  -- ^ Reduction operator (given the sum chunk size)
    -> [(Int, a)]      -- ^ Stack
    -> a               -- ^ Default
    -> [(Int, a)]
push binop stack item =
    merge (1, item) stack
    where
        merge !x [] = [x]
        merge !(i, x) ((j, y) : zs)
            -- | i >= j && (i + j > 1000) = (force y) `par` merge (i+j, binop (i+j) y x) zs
            | i >= j    = merge (i+j, binop (i+j) y x) zs
            | otherwise = (i, x) : (j, y) : zs

-- | Map and reduce in parallel in log stack space
mapReduce :: (NFData b)
    => (a -> b)      -- ^ Map
    -> (Int -> b -> b -> b) -- ^ Reduce (given the sum of the chunk sizes)
    -> b             -- ^ Default (or Identity)
    -> [a]           -- ^ Input
    -> b
mapReduce fn binop def =
    flatten binop def . foldl' (push binop) [] . map fn

-- | Map and reduce in parallel in log stack space
mapReduceParThresh :: (Show b, NFData b)
    => Int     -- ^ Chunk size (to be forced)
    -> (a -> b)      -- ^ Map
    -> (b -> b -> b) -- ^ Reduction
    -> b             -- ^ Default (or Identity)
    -> [a]           -- ^ Input
    -> b
mapReduceParThresh chunksize fn binop def xs =
    mapReduce fn threshBinop def xs
    where
        threshBinop size x y
            | size > chunksize = (zx `par` zy) `pseq` (binop zx zy)
            | otherwise = binop x y
            where
                zx = force x
                zy = y

                
simpleMapReduce :: (NFData b)
    => Int              -- Batch size
    -> (a -> b)         -- Mapper
    -> (b -> b -> b)    -- Reducer
    -> b                -- Default
    -> [a]              -- Input
    -> b
simpleMapReduce _    _  _   def [] = def
simpleMapReduce size mp red def xs =
    (bd `par` ed) `seq` red bd ed
    where
        bd = doChunk begins
        ed = doChunk ends
        (begins, ends) = splitAt size xs
        doChunk = foldl' (\x y -> red x $ mp y) def
        
simpleReduce :: (NFData a)
    => Int              -- Batch size
    -> (a -> a -> a)    -- Reducer
    -> a                -- Default
    -> [a]              -- Input
    -> a
simpleReduce _    _   def [] = def
simpleReduce size red def xs =
    -- for sanity block > chunk > item
    -- block = [chunk], chunk = [item]
    seqFlat blocks
    where
        -- Define the pieces
        shrink :: Int -> ([x] -> x) -> [x] -> [x]
        shrink factor flatten [] = []
        shrink factor flatten b = flatten f : shrink factor flatten r
            where (f, r) = splitAt factor b
                  
        chunks = shrink size seqFlat xs
        
        blocks = shrink caps parFlat chunks
        caps = unsafePerformIO getNumCapabilities
        
        -- Compute the reduction
        parFlat list =
            force $ foldl parRed def list
            where
                parRed x y = (y `par` x) `pseq` red x y
                
        seqFlat list = foldl' red def list
    
    



--conservativeTokenize :: Text.Text -> [Text.Text]
conservativeTokenize = 
    filter (not . Text.null) . Text.split spaceOrPunctuation . Text.toLower 
    where
        spaceOrPunctuation char = HS.member char delimiters 
        delimiters = HS.fromList "\t ~`!@#$%^&*()_+-={}|[]\\:\";'<>?,./"