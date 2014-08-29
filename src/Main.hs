{-
    You'll need:
        murmur-hash
        vector
-}
{-# LANGUAGE BangPatterns #-}
import qualified RIndex.RIndex as RIndex

import Prelude hiding (foldl', foldl1)
import System.IO ( hSetBuffering, BufferMode(NoBuffering), stdout, print)
import System.Environment
import Control.Monad hiding (mapM_)

import Database.HDBC
import Database.HDBC.Sqlite3
import Pipes
import qualified Pipes.Prelude as P
import Control.Parallel.Strategies
import Control.Parallel

import Data.ByteString.Lazy.Builder (int32LE, toLazyByteString)
import Data.Monoid
import Data.Binary
import Text.Printf
import Data.Foldable
import Data.List (foldl1')
import qualified Data.Map.Strict as M
import qualified Data.Vector.Unboxed as V
import qualified Data.Set as S
import qualified Data.Sequence as Seq
        
chunkParagraphs :: Connection -> Producer (Seq.Seq String) IO ()
chunkParagraphs conn = do
    query <- lift $ quickQuery conn "SELECT content FROM paragraphs" []
    nextChunk (0::Int) query
    where
        nextChunk idx [] = return ()
        nextChunk idx content = do
            let (start, end) = splitAt 50000 content
            yield $ Seq.fromList $ fmap (fromSql . head) start
            lift $ (printf "about %i paragraphs fetched\n" idx :: IO () )
            nextChunk (idx+50000) end
            

indexChunk :: RIndex.Bloom -> RIndex.ContextMap -> (Seq.Seq String) -> RIndex.ContextMap
indexChunk bloom contextmap paragraphs =
    foldl' processParagraph contextmap paragraphs
    where
        processParagraph cmap paragraph = RIndex.addBinaryMultiwordContext bloom cmap $ uniqueWords paragraph
        uniqueWords = S.toList . S.fromList . words -- TODO: Do better splitting-}
            
bloomChunk :: (RIndex.Bloom, Int) -> (Seq.Seq String) -> (RIndex.Bloom, Int)
bloomChunk (hist, hist_count) paragraphs =
    if hist_count < 10
       then par merged_new_bloom $ (merged_new_bloom, hist_count + 1)
       else seq hist $ (merged_new_bloom, 2)
    where
        merged_new_bloom =
            seq new_bloom $ RIndex.mergeBlooms new_bloom hist
        new_bloom = RIndex.countChunk $ fmap uniqueWords paragraphs
        --processParagraph bmap paragraph = do
        --    RIndex.addBinaryMultiwordContext bmap $ uniqueWords paragraph
        uniqueWords = S.toList . S.fromList . words -- TODO: Do better splitting


main = do
    text_database:_ <- getArgs 
    conn <- connectSqlite3 text_database
    
    -- pchunks <- chunkParagraphs conn (-1)
    -- putStr $ head $ pchunks
    
     
    -- runEffect $ (chunkParagraphs conn) >-> (bloomChunk Seq.empty)
    putStrLn "Part 1: Counting words (for filtering)"
    end_bloom <- P.fold bloomChunk (RIndex.makeImmBloom, 1) (fst) (chunkParagraphs conn)
    print $ V.sum end_bloom
    
    putStrLn "Part 2: Generating contexts"
    context_map <- P.fold (indexChunk end_bloom) M.empty id (chunkParagraphs conn)
    
    putStrLn "Loading result in database"
    
    
    putStr "\nTotal unique words found: "
    print $ M.size context_map
    
    committer <- prepare conn "INSERT OR REPLACE INTO rindex (word, context) VALUES (?, ?);"
    executeMany committer [ [toSql word, toSql $ encode vec] | (word, vec) <- M.toList context_map]
    commit conn
    disconnect conn