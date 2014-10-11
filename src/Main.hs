{-
    You'll need:
        murmur-hash
        vector
-}
import qualified Index.DistSemantics as DS
import qualified Index.Utility as U
import qualified Index.Popularity as P

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
        chunksize = 100000
        nextChunk idx [] = return ()
        nextChunk idx content = do
            let (start, end) = splitAt chunksize content
            yield $ Seq.fromList $ fmap (fromSql . head) start
            lift (printf "about %i paragraphs fetched\n" (idx + chunksize) :: IO () )
            nextChunk (idx+chunksize) end
            

indexChunk :: P.Popularity -> Seq.Seq Text.Text -> DS.Distributions
indexChunk popularity paragraphs =
    mconcat dist_list
    where
        dist_list = toList $ fmap processParagraph paragraphs
        processParagraph paragraph = DS.mkDistributions $ filter (P.isPopular popularity) $ U.conservativeTokenize paragraph

-- | Count the words prior to making contexts (to save much memory)
bloomChunk :: Seq.Seq Text.Text -> P.Popularity
bloomChunk paragraphs =
    P.trimCount $ P.mergeChunks $ toList $ fmap (P.countChunk . U.conservativeTokenize) paragraphs

{-parFold threads chunkMapper chunks =
    P.fold processChunk (replicate threads mempty) mconcat chunks
    where
        processChunk hists chunk =
            pseq mother $ pseq father $ par me $ (third : others) ++ [me, mempty]
            where
                me = mappend (chunkMapper chunk) $ mappend mother father
                mother : third : father : others = hists
                -}
main = do
    text_database:_ <- getArgs 
    conn <- connectSqlite3 text_database
    
    putStrLn "Part 1: Counting words (for filtering)"
    --end_bloom <- parFold 10 bloomChunk (pullParagraphChunks conn)
    end_bloom <- P.fold (\x y -> mappend x (bloomChunk y)) mempty P.trimCount (pullParagraphChunks conn)
    
    putStrLn $ "Found " ++ show (HM.size end_bloom) ++ " popular words"
    
    putStrLn "Part 2: Generating contexts"
    dist <- P.fold (\x y -> mappend x (indexChunk end_bloom y)) mempty id (pullParagraphChunks conn)
    
    putStrLn "Part 3: Loading result in database"
    
    putStr "\nCollection statistics: "
    print $ DS.stat dist
    
    committer <- prepare conn "INSERT OR REPLACE INTO rindex (word, count, context) VALUES (?, ?, ?);"
    executeMany committer [ [toSql word, toSql cnt, toSql vec] | (word, cnt, vec) <- DS.serialize dist]
    commit conn
    disconnect conn