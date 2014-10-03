{-
    You'll need:
        murmur-hash
        vector
-}
{-# LANGUAGE BangPatterns #-}
import qualified Index.DistSemantics as DS
import Index.Bloom (Bloom(..), countChunk)

import Prelude hiding (foldl', foldl1, concatMap)
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

import Debug.Trace

        
pullParagraphChunks :: Connection -> Producer (Seq.Seq Text.Text) IO ()
pullParagraphChunks conn = do
    query <- lift $ quickQuery conn "SELECT content FROM paragraphs" []
    nextChunk (0::Int) query
    where
        chunksize = 1000
        nextChunk idx [] = return ()
        nextChunk idx content = do
            let (start, end) = splitAt chunksize content
            yield $ Seq.fromList $ fmap (fromSql . head) start
            lift $ (printf "about %i paragraphs fetched\n" (idx + chunksize) :: IO () )
            nextChunk (idx+chunksize) end
            

indexChunk :: Bloom -> (Seq.Seq Text.Text) -> DS.Distributions
indexChunk bloom paragraphs =
    mconcat $ toList $ fmap processParagraph paragraphs
    where
        processParagraph paragraph = DS.getDistributions bloom $ uniqueWords paragraph
        uniqueWords = S.toList . S.fromList . Text.words -- TODO: Do better splitting-}

-- | Convert paragraphs to word sets, so that we are counting
-- the number of `unique paragraphs` where the word appears.
-- e.g. "me me me me me" is [("me", 1)] but "me\n\nme\n\nme" is [("me", 3)]
bloomChunk :: (Seq.Seq Text.Text) -> Bloom
bloomChunk paragraphs =
    countChunk $ concatMap uniqueWords paragraphs
    where
        uniqueWords = S.toList . S.fromList . Text.words -- TODO: Do better splitting


parFold threads chunkMapper chunks =
    P.fold processChunk (replicate threads mempty) mconcat chunks
    where
        processChunk hists chunk =
            pseq mother $ pseq father $ par me $ (third : others) ++ [me, mempty]
            where
                me = mappend (chunkMapper chunk) $ mappend mother father
                mother : third : father : others = hists

main = do
    text_database:_ <- getArgs 
    conn <- connectSqlite3 text_database
    
    putStrLn "Part 1: Counting words (for filtering)"
    end_bloom <- parFold 10 bloomChunk (pullParagraphChunks conn)
    
    putStrLn "Part 2: Generating contexts"
    dist <- parFold 10 (indexChunk end_bloom) (pullParagraphChunks conn)
    
    putStrLn "Loading result in database"
    
    
    putStr "\nTotal unique words found: "
    print $ DS.stat dist
    
    committer <- prepare conn "INSERT OR REPLACE INTO rindex (word, context) VALUES (?, ?);"
    executeMany committer [ [toSql word, toSql vec] | (word, vec) <- DS.serialize dist]
    commit conn
    disconnect conn