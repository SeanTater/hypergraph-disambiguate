{-# LANGUAGE BangPatterns #-}
{-
    You'll need:
        murmur-hash
        vector
-}
import qualified Index.DistSemantics as DS
import qualified Index.Utility as U
import qualified Index.Popularity as P

import Prelude hiding (foldl', foldl1, concatMap, mapM_)
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
import qualified Data.Text as Text
import qualified Data.HashMap.Strict as HM
import qualified Data.Vector.Unboxed as V
import qualified Data.Set as S
import qualified Data.Sequence as Seq
import qualified Data.Stream as St
import System.ProgressBar
import System.IO ( hSetBuffering, BufferMode(NoBuffering), stdout )
import System.CPUTime
import Control.DeepSeq


        
pullParagraphChunks :: Connection -> Producer (Text.Text) IO ()
pullParagraphChunks conn = do
    query <- lift $ quickQuery conn "SELECT content FROM paragraphs LIMIT 10000" []
    void $ foldM processParagraph 0 query
    where
        processParagraph count item = do
            when (count `rem` 1000 == 0)
                (lift $ progressBar (msg "Paragraphs finished") exact 80 count 43000000)
            (yield.fromSql.head) item
            return $ count + 1
            
pullParagraphChunksL :: Connection -> IO [Text.Text]
pullParagraphChunksL conn = do
    query <- quickQuery conn "SELECT content FROM paragraphs" []
    mapM maybeNotify $ zip [0..] query
    where
        maybeNotify :: (Integer, [SqlValue]) -> IO Text.Text
        maybeNotify (count, item) = do
            when (count `rem` 1000 == 0)
               (progressBar (msg "Paragraphs finished") exact 80 count 43000000)
            (return.fromSql.head) item
            
pullParagraphChunksL' :: Connection -> IO [Text.Text]
pullParagraphChunksL' conn = do
    query <- quickQuery conn "SELECT content FROM paragraphs" []
    return $ map (fromSql.head) query

indexChunk :: P.Popularity -> Text.Text -> DS.Distributions
indexChunk popularity =
    DS.mkDistributions . filter (P.isPopular popularity) . U.conservativeTokenize

-- | Count the words prior to making contexts (to save much memory)
bloomChunk :: Text.Text -> P.Popularity
bloomChunk = P.countChunk . U.conservativeTokenize

logTime :: IO () -> IO ()
logTime action = do
    start <- getCPUTime
    action
    end <- getCPUTime
    let diff = (fromIntegral end - fromIntegral start) / 1e12 :: Double
    putStrLn $ "Seconds elapsed: " ++ show diff ++ "\n"
    


main = do
    text_database:_ <- getArgs 
    conn <- connectSqlite3 text_database
    hSetBuffering stdout NoBuffering -- For progressBar
    
    putStrLn "Part 1: Counting words (for filtering)"
    --end_bloom <- parFold 10 bloomChunk (pullParagraphChunks conn)
    --end_bloom <- P.fold (\x y -> P.appendChunk x (bloomChunk y)) mempty P.trimCount (pullParagraphChunks conn)
    
    paragraphs <- pullParagraphChunksL' conn
    --let end_bloom = P.trimCount $ P.mergeChunks $ fmap bloomChunk $ paragraphs
    let end_bloom = U.mapReduceParThresh 10000 bloomChunk P.appendChunk mempty paragraphs
    
    --end_bloom <- St.foldl' (\x y -> mappend x (bloomChunk y)) 
    
    
    logTime $ putStrLn $ "Found " ++ show (HM.size end_bloom) ++ " popular words"
    
    putStrLn "Part 2: Generating contexts"
    dist <- P.fold (\x y -> mappend x (indexChunk end_bloom y)) mempty id (pullParagraphChunks conn)
    
    putStrLn "Part 3: Loading result in database"
    
    putStr "\nCollection statistics: "
    print $ DS.stat dist
    
    committer <- prepare conn "INSERT OR REPLACE INTO rindex (word, count, context) VALUES (?, ?, ?);"
    executeMany committer [ [toSql word, toSql cnt, toSql vec] | (word, cnt, vec) <- DS.serialize dist]
    commit conn
    disconnect conn