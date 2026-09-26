{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeOperators #-}

module DataFrame.IO.Parquet.Writer (
    writeParquet,
    writeParquetWithOptions,
    writeParquetEff,
    writeParquetWithOptionsEff,
    ParquetWriteOptions (..),
    WriterStrategy (..),
    defaultParquetWriteOptions,
    nativeTypeKeyPrefix,
    nativeTypeKeyValues,
) where

import Bluefin.Eff (Eff, type (<:))
import Bluefin.IO (IOE, effIO, runEff)
import qualified Bluefin.Prim as P
import Control.Monad (forM_, unless, when)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Primitive (PrimMonad, PrimState)
import qualified Data.ByteString as BS
import Data.Int (Int64)
import Data.Maybe (fromJust)
import Data.Primitive.ByteArray (getSizeofMutableByteArray)
import Data.Primitive.MutVar (
    MutVar,
    modifyMutVar',
    newMutVar,
    readMutVar,
    writeMutVar,
 )
import qualified Data.Text as T
import qualified Data.Vector as VB
import DataFrame.IO.Parquet.Thrift hiding (schema)
import DataFrame.IO.Parquet.Writer.DefLevels (
    DefLevels (..),
    flushDef,
    newDefLevels,
    pushDef,
 )
import DataFrame.IO.Parquet.Writer.Encoder (Encoder (..), buildEncoder)
import DataFrame.IO.Parquet.Writer.Metadata (
    magic,
    mkColumnChunk,
    mkDataPageHeader,
    mkRowGroup,
    mkSchemaElem,
    rootSchemaElement,
    writeFooter,
 )
import DataFrame.IO.Parquet.Writer.Options (
    ParquetWriteOptions (..),
    WriterStrategy (..),
    defaultParquetWriteOptions,
 )
import DataFrame.IO.Parquet.Writer.PrimMonad (PrimM, runPrimM)
import DataFrame.IO.Utils.RandomAccess (
    MemoryBuffer (..),
    WritableBinaryHandle,
    atomicallyWriteFile,
    bufferResidency,
    bufferToByteString,
    ensureCapacity,
    flushBufferToBuffer,
    flushBufferToFile,
    mallocBuffer,
    resetPosition,
    withWritableBinaryFile,
    writeByteString,
    writeByteStringToFile,
    writeWord32LE,
 )
import DataFrame.Internal.Column (Column, columnTypeString, hasMissing)
import DataFrame.Internal.DataFrame (
    DataFrame,
    columnNames,
    dataframeDimensions,
    getColumn,
 )
import qualified Pinch
import qualified Snappy
import System.Directory (createDirectoryIfMissing)
import System.FilePath (takeDirectory)
import Text.Printf (printf)

data ParquetWriterState e1 e3 = ParquetWriterState
    { outputFileHandle :: !WritableBinaryHandle
    , columnChunks :: !(VB.Vector (ColumnChunkState e1 e3))
    , currentFileOffsetRef :: !(MutVar (P.PrimStateEff e1) Int64)
    , scratchBuffer :: !(MemoryBuffer (P.PrimStateEff e1))
    , rowGroupMetadataRef :: !(MutVar (P.PrimStateEff e1) [RowGroup])
    , rowNumberRef :: !(MutVar (P.PrimStateEff e1) Int)
    }

data ColumnChunkState e1 e3 = ColumnChunkState
    { columnName :: !T.Text
    , nullable :: !Bool
    , schema :: !SchemaElement
    , encoder :: !(Encoder e1 e3)
    , buffer :: !(MemoryBuffer (P.PrimStateEff e1))
    , uncompressedBufferSize :: !(MutVar (P.PrimStateEff e1) Int64)
    , pageState :: !(PageState (PrimM e1))
    }

data PageState m = PageState
    { pageBuffer :: !(MemoryBuffer (PrimState m))
    , definitionLevels :: !(DefLevels (PrimState m))
    , currentRowCount :: !(MutVar (PrimState m) Int)
    }

writeParquet :: FilePath -> DataFrame -> IO ()
writeParquet = writeParquetWithOptions defaultParquetWriteOptions

writeParquetWithOptions :: ParquetWriteOptions -> FilePath -> DataFrame -> IO ()
writeParquetWithOptions options path df =
    runEff $ \ioe -> writeParquetWithOptionsEff ioe options path df

-- | Bluefin version of 'writeParquet'.
writeParquetEff :: (e1 <: e3) => IOE e1 -> FilePath -> DataFrame -> Eff e3 ()
writeParquetEff ioe = writeParquetWithOptionsEff ioe defaultParquetWriteOptions

{- | Bluefin version of 'writeParquetWithOptions'. The writer's mutable
buffers are scoped by Bluefin's primitive effect, while file operations use
the supplied IO capability.
-}
writeParquetWithOptionsEff ::
    forall e1 e3.
    (e1 <: e3) =>
    IOE e1 -> ParquetWriteOptions -> FilePath -> DataFrame -> Eff e3 ()
writeParquetWithOptionsEff ioe options path df = do
    when (options.strategy == TwoPass) $
        error
            "The Two Pass Strategy for the Parquet Writer has not yet been implemented"
    case options.compressionCodec of
        UNCOMPRESSED _ -> pure ()
        SNAPPY _ -> pure ()
        other -> error ("writeParquet: unsupported codec " <> show other)
    let (totalRows, _) = dataframeDimensions df
    P.runPrim $ \prim -> do
        let writeShard options shardPath df startRow endRow =
                writeShardIO ioe prim options shardPath df startRow endRow
        case options.maxRowsPerFile of
            Nothing -> do
                when (isShardPattern path) $
                    error
                        ( "writeParquet: path "
                            <> show path
                            <> " contains a '*' placeholder but maxRowsPerFile is not set"
                        )
                writeShard options path df 0 totalRows
            Just rowsPerFile -> do
                when (rowsPerFile <= 0) $
                    error "writeParquet: maxRowsPerFile must be positive"
                unless (isShardPattern path) $
                    error
                        ( "writeParquet: maxRowsPerFile requires a path with a '*' placeholder, got "
                            <> show path
                        )
                let starts = case [0, rowsPerFile .. totalRows - 1] of
                        [] -> [0] -- empty frame still produces one (empty) shard
                        ss -> ss
                forM_ (zip [0 ..] starts) $ \(shardIndex, start) -> do
                    let shardPath = shardPathFor path shardIndex
                    effIO ioe $ createDirectoryIfMissing True (takeDirectory shardPath)
                    writeShard options shardPath df start (min totalRows (start + rowsPerFile))

isShardPattern :: FilePath -> Bool
isShardPattern = elem '*'

-- | Replace every @*@ in the pattern with a zero-padded shard index.
shardPathFor :: FilePath -> Int -> FilePath
shardPathFor pattern_ shardIndex =
    concatMap (\c -> if c == '*' then printf "%05d" shardIndex else [c]) pattern_

-- | Write rows @[startRow, endRow)@ of the frame to a single Parquet file.
writeShardIO ::
    forall e1 e2 e3.
    (e1 <: e3, e2 <: e3) =>
    IOE e1 ->
    P.Prim e2 e2 ->
    ParquetWriteOptions ->
    FilePath ->
    DataFrame ->
    Int ->
    Int ->
    Eff e3 ()
writeShardIO ioe prim options path_ df startRow endRow = do
    let names = columnNames df
        shardRows = max 0 (endRow - startRow)
    columnChunks_ <-
        VB.fromList
            <$> mapM
                ( \columnName_ ->
                    initColumnChunkState
                        ioe
                        prim
                        options
                        columnName_
                        (fromJust (getColumn columnName_ df))
                )
                names
    scratchBuffer_ <- runPrimM ioe prim (mallocBuffer (max 1 options.pageSize))
    atomicallyWriteFile ioe path_ $ \path ->
        withWritableBinaryFile ioe path $ \output -> do
            effIO ioe $ writeByteStringToFile output magic
            currentFileOffsetRef_ <- runPrimM ioe prim (newMutVar 4)
            rowGroupMetadataRef_ <- runPrimM ioe prim (newMutVar [])
            rowNumberRef_ <- runPrimM ioe prim (newMutVar 0)
            let writerState =
                    ParquetWriterState
                        output
                        columnChunks_
                        currentFileOffsetRef_
                        scratchBuffer_
                        rowGroupMetadataRef_
                        rowNumberRef_
                interval = max 1 options.batchRows
                subBatch = max 1 options.subBatchRows
                writeBatch rowNum batchEnd
                    | rowNum >= batchEnd = pure ()
                    | otherwise = do
                        let count = min subBatch (batchEnd - rowNum)
                        VB.forM_ columnChunks_ (writeRows ioe prim options scratchBuffer_ rowNum count)
                        runPrimM ioe prim (modifyMutVar' rowNumberRef_ (+ count))
                        writeBatch (rowNum + count) batchEnd
                loop rowNum
                    | rowNum >= endRow = pure ()
                    | otherwise = do
                        let batchEnd = rowNum + min interval (endRow - rowNum)
                        writeBatch rowNum batchEnd
                        size <- bufferedSize ioe prim columnChunks_
                        when (size >= options.rowGroupSize) $
                            flushRowGroup ioe prim options writerState
                        loop batchEnd
            loop startRow
            flushRowGroup ioe prim options writerState
            rowGroupMetadata <-
                reverse <$> runPrimM ioe prim (readMutVar rowGroupMetadataRef_)
            let schemaElements =
                    rootSchemaElement (VB.length columnChunks_)
                        : VB.toList (VB.map schema columnChunks_)
            runPrimM ioe prim $
                writeFooter
                    output
                    schemaElements
                    shardRows
                    rowGroupMetadata
                    (nativeTypeKeyValues names df)

nativeTypeKeyPrefix :: T.Text
nativeTypeKeyPrefix = "dataframe.type."

-- | The type stamp for every column of @df@, as footer key-value pairs.
nativeTypeKeyValues :: [T.Text] -> DataFrame -> [(T.Text, T.Text)]
nativeTypeKeyValues names df =
    [ (nativeTypeKeyPrefix <> name, T.pack (columnTypeString col))
    | name <- names
    , Just col <- [getColumn name df]
    ]

writeRows ::
    forall e1 e2 e3.
    (e1 <: e3, e2 <: e3) =>
    IOE e1 ->
    P.Prim e2 e2 ->
    ParquetWriteOptions ->
    MemoryBuffer (P.PrimStateEff e2) ->
    Int ->
    Int ->
    ColumnChunkState e2 e3 ->
    Eff e3 ()
writeRows ioe prim options scratch firstRow count ccs = do
    let page = ccs.pageState
        buf = page.pageBuffer
        encode = ccs.encoder.encodeValue
        dl = page.definitionLevels
        end = firstRow + count

    pos0 <- runPrimM ioe prim (readMutVar buf.positionRef)
    let margin = options.pageSize
    arr0 <- runPrimM ioe prim (ensureCapacity buf (pos0 + max margin (count * 64)))
    size0 <- runPrimM ioe prim (getSizeofMutableByteArray arr0)

    let go !size !pos !row
            | row >= end = runPrimM ioe prim (writeMutVar buf.positionRef pos)
            | pos + margin > size = do
                -- Rare: buffer nearly full, grow it
                size' <-
                    runPrimM ioe prim $ do
                        writeMutVar buf.positionRef pos
                        arr' <-
                            ensureCapacity
                                buf
                                (pos + max margin ((end - row) * 64))
                        getSizeofMutableByteArray arr'
                go size' pos row
            | otherwise = do
                (pos', notNull) <- encode buf pos row
                when ccs.nullable $
                    runPrimM ioe prim (pushDef dl (if notNull then 1 else 0))
                go size pos' (row + 1)

    go size0 pos0 firstRow

    -- Batch bookkeeping: once per sub-batch instead of per value
    (pageRes, defRes) <- runPrimM ioe prim $ do
        modifyMutVar' page.currentRowCount (+ count)
        flushDef dl
        pageRes <- bufferResidency buf
        defRes <- bufferResidency dl.dlBuf
        pure (pageRes, defRes)
    when
        (pageRes + defRes >= options.pageSize)
        (flushPage ioe prim options scratch ccs)

flushPage ::
    forall e1 e2 e3.
    (e1 <: e3, e2 <: e3) =>
    IOE e1 ->
    P.Prim e2 e2 ->
    ParquetWriteOptions ->
    MemoryBuffer (P.PrimStateEff e2) ->
    ColumnChunkState e2 e3 ->
    Eff e3 ()
flushPage ioe prim options scratch columnChunkState = do
    let page = columnChunkState.pageState
    numPageRows <- runPrimM ioe prim (readMutVar page.currentRowCount)
    when (numPageRows > 0) $ do
        pos <- runPrimM ioe prim (readMutVar page.pageBuffer.positionRef)
        pos' <- columnChunkState.encoder.finishValues page.pageBuffer pos
        runPrimM ioe prim (writeMutVar page.pageBuffer.positionRef pos')
        body <- assemblePageBody ioe prim scratch columnChunkState
        writeDataPage
            ioe
            prim
            options.compressionCodec
            numPageRows
            body
            columnChunkState
        runPrimM ioe prim $ do
            resetPosition page.pageBuffer
            resetPosition page.definitionLevels.dlBuf
            resetPosition scratch
            writeMutVar page.currentRowCount 0

assemblePageBody ::
    forall e1 e2 e3.
    (e1 <: e3, e2 <: e3) =>
    IOE e1 ->
    P.Prim e2 e2 ->
    MemoryBuffer (P.PrimStateEff e2) ->
    ColumnChunkState e2 e3 ->
    Eff e3 (MemoryBuffer (P.PrimStateEff e2))
assemblePageBody ioe prim scratch columnChunkState
    | not columnChunkState.nullable = pure columnChunkState.pageState.pageBuffer
    | otherwise = do
        let page = columnChunkState.pageState
        runPrimM ioe prim $ do
            flushDef page.definitionLevels
            resetPosition scratch
            defLevelsSize <- bufferResidency page.definitionLevels.dlBuf
            writeWord32LE scratch (fromIntegral defLevelsSize)
            flushBufferToBuffer page.definitionLevels.dlBuf scratch
            flushBufferToBuffer page.pageBuffer scratch
        pure scratch

writeDataPage ::
    forall e1 e2 e3.
    (e1 <: e3, e2 <: e3) =>
    IOE e1 ->
    P.Prim e2 e2 ->
    CompressionCodec ->
    Int ->
    MemoryBuffer (P.PrimStateEff e2) ->
    ColumnChunkState e2 e3 ->
    Eff e3 ()
writeDataPage ioe prim codec numPageRows body columnChunkState = do
    uncompressedPageSize <- runPrimM ioe prim (bufferResidency body)
    compressedBody <- case codec of
        UNCOMPRESSED _ -> pure Nothing
        SNAPPY _ ->
            Just . Snappy.compress <$> runPrimM ioe prim (bufferToByteString body)
        other -> error ("writeParquet: unsupported codec " <> show other)
    let compressedPageSize = maybe uncompressedPageSize BS.length compressedBody
        headerBytes =
            Pinch.encode
                Pinch.compactProtocol
                (mkDataPageHeader numPageRows uncompressedPageSize compressedPageSize)
    runPrimM ioe prim $ do
        writeByteString columnChunkState.buffer headerBytes
        case compressedBody of
            Nothing -> flushBufferToBuffer body columnChunkState.buffer
            Just bytes -> writeByteString columnChunkState.buffer bytes
        modifyMutVar'
            columnChunkState.uncompressedBufferSize
            (+ fromIntegral (BS.length headerBytes + uncompressedPageSize))

flushRowGroup ::
    forall e1 e2 e3.
    (e1 <: e3, e2 <: e3) =>
    IOE e1 ->
    P.Prim e2 e2 ->
    ParquetWriteOptions ->
    ParquetWriterState e2 e3 ->
    Eff e3 ()
flushRowGroup ioe prim options writerState = do
    rowNumber <- runPrimM ioe prim (readMutVar writerState.rowNumberRef)
    when (rowNumber > 0) $ do
        VB.forM_
            writerState.columnChunks
            (flushPage ioe prim options writerState.scratchBuffer)
        (reversedColumnChunks, totalCompressed, totalUncompressed) <-
            VB.foldM'
                ( \(acc, totalCompressedSize, totalUncompressedSize) columnChunkState -> do
                    (offset, compressedSize, uncompressedSize) <-
                        runPrimM ioe prim $ do
                            offset <-
                                readMutVar writerState.currentFileOffsetRef
                            compressedSize <-
                                bufferResidency columnChunkState.buffer
                            uncompressedSize <-
                                readMutVar
                                    columnChunkState.uncompressedBufferSize
                            flushBufferToFile
                                writerState.outputFileHandle
                                columnChunkState.buffer
                            writeMutVar
                                writerState.currentFileOffsetRef
                                (offset + fromIntegral compressedSize)
                            writeMutVar
                                columnChunkState.uncompressedBufferSize
                                0
                            pure (offset, compressedSize, uncompressedSize)
                    let columnChunk =
                            mkColumnChunk
                                options.compressionCodec
                                columnChunkState.encoder.encType
                                columnChunkState.columnName
                                offset
                                compressedSize
                                uncompressedSize
                                rowNumber
                    pure
                        ( columnChunk : acc
                        , totalCompressedSize + fromIntegral compressedSize
                        , totalUncompressedSize + uncompressedSize
                        )
                )
                ([], 0 :: Int64, 0 :: Int64)
                writerState.columnChunks
        runPrimM ioe prim $ do
            modifyMutVar'
                writerState.rowGroupMetadataRef
                ( mkRowGroup
                    (reverse reversedColumnChunks)
                    totalCompressed
                    totalUncompressed
                    rowNumber
                    :
                )
            writeMutVar writerState.rowNumberRef 0

bufferedSize ::
    (e1 <: e3, e2 <: e3) =>
    IOE e1 ->
    P.Prim e2 e2 ->
    VB.Vector (ColumnChunkState e2 e3) ->
    Eff e3 Int
bufferedSize ioe prim =
    VB.foldM'
        ( \total columnChunkState -> do
            (chunkSize, valuesSize, defLevelsSize) <- runPrimM ioe prim $ do
                chunkSize <- bufferResidency columnChunkState.buffer
                valuesSize <-
                    bufferResidency columnChunkState.pageState.pageBuffer
                defLevelsSize <-
                    bufferResidency
                        columnChunkState.pageState.definitionLevels.dlBuf
                pure (chunkSize, valuesSize, defLevelsSize)
            pure (total + chunkSize + valuesSize + defLevelsSize)
        )
        0

initColumnChunkState ::
    (e1 <: e3, e2 <: e3) =>
    IOE e1 ->
    P.Prim e2 e2 ->
    ParquetWriteOptions ->
    T.Text ->
    Column ->
    Eff e3 (ColumnChunkState e2 e3)
initColumnChunkState ioe prim options columnName_ column = do
    encoder_ <- buildEncoder ioe prim column
    let nullable_ = hasMissing column
        schema_ =
            mkSchemaElem
                columnName_
                encoder_.encType
                nullable_
                encoder_.convertedType
                encoder_.logicalType
        bufferSize = max 1 options.pageSize
    -- ColumnChunk Buffers start at page size and grow to their
    -- actual size over the course of building out the first row
    -- group.
    -- Each column chunk in a row group must have the same number
    -- of rows, but each column chunk is liable to fit the same
    -- number of rows in varying amounts of data depending on the
    -- encoding and the compression characteristics of the data.
    -- So the optimal buffer size of each column chunk is liable
    -- to vary
    -- As a result while one specific column chunk in a row group
    -- is likely to hit the page limit, the others are liable to be
    -- much smaller than the limit.
    (buffer_, uncompressedBufferSize_, pageState_) <-
        runPrimM ioe prim $ do
            buffer_ <- mallocBuffer bufferSize
            uncompressedBufferSize_ <- newMutVar 0
            pageState_ <- initPageState bufferSize
            pure (buffer_, uncompressedBufferSize_, pageState_)
    pure
        ColumnChunkState
            { columnName = columnName_
            , nullable = nullable_
            , schema = schema_
            , encoder = encoder_
            , buffer = buffer_
            , uncompressedBufferSize = uncompressedBufferSize_
            , pageState = pageState_
            }

initPageState :: (PrimMonad m, MonadIO m) => Int -> m (PageState m)
initPageState bufferSize = do
    pageBuffer_ <- mallocBuffer bufferSize
    definitionLevels_ <- newDefLevels
    currentRowCount_ <- newMutVar 0
    pure
        PageState
            { pageBuffer = pageBuffer_
            , definitionLevels = definitionLevels_
            , currentRowCount = currentRowCount_
            }
