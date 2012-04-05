module Data.Conduit.Tar
    (
    -- * USTAR Header & Blocks
    Block(..),
    Header(..),
    TypeFlag(..),

    -- * Conduits and Sinks
    blocks,
    extract
    )
where

import Control.Applicative
import Control.Monad.IO.Class
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Data.Conduit
import qualified Data.Foldable as F
import qualified Data.Sequence as S
import Data.Serialize
import Data.Word
import System.Directory
import System.IO

data Block = BlockBytes B.ByteString -- ^ A bytestring chunk of 512 bytes
             | BlockHeader Header
             deriving (Eq, Show, Read)

-- | Header for the USTAR format. Perhaps I'll support the old UNIX tar
-- format later, but its not too important to me right now.
data Header = Header {
    headerName :: B.ByteString, -- ^ 100 bytes long
    headerMode :: Word64,
    headerOwnerUID :: Word64,
    headerOwnerGID :: Word64,
    headerFileSize :: Integer, -- ^ 12 bytes
    headerModifyTime :: Integer, -- ^ 12 bytes
    headerChecksum :: Word64,
    headerType :: TypeFlag, -- ^ 1 byte
    headerLinkName :: B.ByteString, -- ^ 100 bytes
    headerMagic :: B.ByteString, -- ^ 6 bytes
    headerVersion :: Word16,
    headerOwnerUserName :: B.ByteString, -- ^ 32 bytes
    headerOwnerGroupName :: B.ByteString, -- ^ 32 bytes
    headerDeviceMajorNumber :: Word64,
    headerDeviceMinorNumber :: Word64,
    headerFilenamePrefix :: B.ByteString -- ^ 155 bytes
    }
    deriving (Eq, Show, Read)

data TypeFlag = NormalFile
                | HardLink
                | SymbolicLink
                | CharacterSpecial
                | BlockSpecial
                | Directory
                | NamedPipe
                | ContiguousFile
                | FileMetaData
                | GlobalMetaData
                | VendorSpecificExtension Word8
                | Unknown Word8
                deriving (Eq, Show, Read)

-- | Parse the first 500 bytes of the header and skip the rest
parseHeader :: B.ByteString -> Header
parseHeader bytes = case h of
                        Left e -> error $ "Bad header: " ++ e
                        Right h' -> h'
    where h = flip runGet bytes $ Header
                <$> getBytes' 100   -- name
                <*> getOctal 8      -- mode
                <*> getOctal 8      -- owner uid
                <*> getOctal 8      -- owner gid
                <*> getOctal 12     -- file size
                <*> getOctal 12     -- modify time
                <*> getOctal 8      -- checksum
                <*> getTypeFlag     -- type flag
                <*> getBytes' 100   -- link name
                <*> getBytes' 6     -- magic
                <*> getOctal 2      -- version
                <*> getBytes' 32    -- user name
                <*> getBytes' 32    -- group name
                <*> getOctal 8      -- device major number
                <*> getOctal 8      -- device minor number
                <*> getBytes' 155   -- filename prefix
    
getTypeFlag :: Get TypeFlag
getTypeFlag = parseFlag <$> getWord8

getBytes' :: Int -> Get B.ByteString
getBytes' n = chompEnd <$> getBytes n

parseFlag :: Word8 -> TypeFlag
parseFlag 48 = NormalFile
parseFlag 49 = HardLink
parseFlag 50 = SymbolicLink
parseFlag 51 = CharacterSpecial
parseFlag 52 = BlockSpecial
parseFlag 53 = Directory
parseFlag 54 = NamedPipe
parseFlag 55 = ContiguousFile
parseFlag 103 = GlobalMetaData
parseFlag 120 = FileMetaData
parseFlag n | n <= 90 && n >= 65 = VendorSpecificExtension n
            | otherwise = Unknown n

-- | Get an octal number of a certain number of bytes
getOctal :: Integral i => Int -> Get i
getOctal n = isolate n $ do
    bytes <- mapM (const getWord8) [1..n]
    return . octalToIntegral . reverse . init $ bytes

-- First byte is the least significace
octalToIntegral :: Integral i => [Word8] -> i
octalToIntegral [] = 0
octalToIntegral (x:xs) = octalToIntegral xs * 8 + fromIntegral (octalByte x)

octalByte :: Word8 -> Word8
octalByte n = n - 48

-- | Chomps off trailing 0's from a bytestring
chompEnd :: B.ByteString -> B.ByteString
chompEnd bytes | B.null bytes = B.empty
               | B.last bytes == 0 = chompEnd $ B.init bytes
               | otherwise = bytes

-- | Extracts blocks from a tar file.
blocks :: Monad m => Conduit B.ByteString m Block
blocks = rechunk =$= blocks'

-- | Invariant: The input bytes to this are guaranteed to be 512 bytes
-- in length. Implemented similarly to a map, but has to keep track of
-- whether or not it should read a header, so it can't just use
-- the builtin map.
blocks' :: Monad m => Conduit B.ByteString m Block
blocks' = conduitState 0 push close
    where push n input | B.head input == 0 = return $ StateFinished Nothing []
                       | otherwise = push' n input
            where push' 0 input = return $ StateProducing nchunks output
                    where header = parseHeader input
                          nchunks = numberChunks header
                          output = [BlockHeader header]
                  push' n input = return $ StateProducing (n - 1) output
                    where output = blockBytes $ chompEnd input
          close _ = return []

blockBytes :: B.ByteString -> [Block]
blockBytes bytes | B.null bytes = []
                 | otherwise = [BlockBytes bytes]

numberChunks :: Header -> Integer
numberChunks h = case headerType h of
    NormalFile -> let s = headerFileSize h
                  in if s `mod` 512 == 0
                    then s `div` 512
                    else (s `div` 512) + 1
    _ -> 0

-- | Rechunk a stream of strict @B.ByteString@'s into @B.ByteString@'s of
-- length 512 bytes (since this is what tar uses).
rechunk :: Monad m => Conduit B.ByteString m B.ByteString
rechunk = conduitState B.empty push close
    where push leftover input = return $ StateProducing leftover' (F.toList output)
            where input' = leftover `B.append` input
                  (output, leftover') = splitBytes input'
          close leftover = if B.null leftover
                                then return []
                                else error "not multiple of 512"

-- | Split a @B.ByteString@ into a list of 512 @B.ByteString@'s and
-- possibly a remainder (which will be @B.empty@ if there is no
-- remainder)
splitBytes :: B.ByteString -> (S.Seq B.ByteString, B.ByteString)
splitBytes bytes = let (x, y) = go bytes (S.empty, B.empty)
                   in (x, y)
    where go bytes (acc, _) | B.length bytes < 512 = (acc, bytes)
                            | B.length bytes == 512 = (acc S.|> bytes, B.empty)
                            | otherwise =
                                let (chunk, rest) = B.splitAt 512 bytes
                                in go rest (acc S.|> chunk, B.empty)

extract :: MonadIO m => Sink Block m ()
extract = sinkState Nothing push close
    where push Nothing (BlockHeader header) = case headerType header of
            Directory -> do
                liftIO . createDirectory . BC.unpack . headerName $ header
                return $ StateProcessing Nothing 
            NormalFile -> do
                handle <- liftIO $ openFile (BC.unpack $ headerName header) WriteMode
                return . StateProcessing $ Just handle
            _ -> return $ StateProcessing Nothing
          push (Just handle) (BlockHeader header) = do
            liftIO $ hFlush handle >> hClose handle
            push Nothing (BlockHeader header)
          push (Just handle) (BlockBytes bytes) = do
            liftIO $ B.hPut handle bytes
            return . StateProcessing $ Just handle
          close (Just handle) = liftIO $ hClose handle
          close Nothing = return ()
