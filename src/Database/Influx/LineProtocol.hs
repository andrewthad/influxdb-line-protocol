{-# language ScopedTypeVariables #-}
{-# language RankNTypes #-}
{-# language TypeFamilies #-}
{-# language MagicHash #-}
{-# language BangPatterns #-}
{-# language StandaloneDeriving #-}
{-# language DerivingStrategies #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language PolyKinds #-}
{-# language TypeInType #-}
{-# language TypeApplications #-}

module Database.Influx.LineProtocol
  ( -- * InfluxDB Types
    Point(..)
  , Measurement
  , Tags
  , Fields
  , TagKey
  , TagValue
  , FieldKey
  , FieldValue
    -- * Builder
  , encodePoint
    -- * Escaping
  , escapeMeasurement
  , tagKey
  , tagValue
  , fieldKey
  , fieldValue
  , fieldValueWord64
    -- * Construct Tags
  , tags1
  , fields1
  ) where

import Control.Monad.ST (ST,runST)
import Data.Coerce (coerce)
import Data.Char (ord)
import Data.Bytes.Types (MutableBytes(..))
import Data.Word (Word8,Word64)
import Data.Primitive (ByteArray,MutableByteArray)
import Data.Primitive.Unlifted.Array (UnliftedArray(..))
import Data.Primitive.Unlifted.Class (PrimUnlifted(..))
import GHC.TypeLits (natVal)
import GHC.Exts (RealWorld)

import qualified Data.Primitive as PM
import qualified Data.Primitive.Unlifted.Array as PM
import qualified Data.ByteArray.Builder.Small as BB
import qualified Data.ByteArray.Builder.Small.Unsafe as BBU

-- | InfluxDB measurement name. In the byte array that backs this,
-- commas and spaces have been prefixed by backslashes.
newtype Measurement = Measurement ByteArray

-- | InfluxDB tag key.
newtype TagKey = TagKey ByteArray

instance PrimUnlifted TagKey where
  type Unlifted TagKey = PM.ByteArray#
  toUnlifted# = coerce (toUnlifted# @ByteArray)
  fromUnlifted# = coerce (fromUnlifted# @ByteArray)
  indexUnliftedArray# = coerce (indexUnliftedArray# @ByteArray)
  readUnliftedArray# = coerce (readUnliftedArray# @ByteArray)
  writeUnliftedArray# = coerce (writeUnliftedArray# @ByteArray)

instance PrimUnlifted TagValue where
  type Unlifted TagValue = PM.ByteArray#
  toUnlifted# = coerce (toUnlifted# @ByteArray)
  fromUnlifted# = coerce (fromUnlifted# @ByteArray)
  indexUnliftedArray# = coerce (indexUnliftedArray# @ByteArray)
  readUnliftedArray# = coerce (readUnliftedArray# @ByteArray)
  writeUnliftedArray# = coerce (writeUnliftedArray# @ByteArray)

instance PrimUnlifted FieldKey where
  type Unlifted FieldKey = PM.ByteArray#
  toUnlifted# = coerce (toUnlifted# @ByteArray)
  fromUnlifted# = coerce (fromUnlifted# @ByteArray)
  indexUnliftedArray# = coerce (indexUnliftedArray# @ByteArray)
  readUnliftedArray# = coerce (readUnliftedArray# @ByteArray)
  writeUnliftedArray# = coerce (writeUnliftedArray# @ByteArray)

instance PrimUnlifted FieldValue where
  type Unlifted FieldValue = PM.ByteArray#
  toUnlifted# = coerce (toUnlifted# @ByteArray)
  fromUnlifted# = coerce (fromUnlifted# @ByteArray)
  indexUnliftedArray# = coerce (indexUnliftedArray# @ByteArray)
  readUnliftedArray# = coerce (readUnliftedArray# @ByteArray)
  writeUnliftedArray# = coerce (writeUnliftedArray# @ByteArray)

-- | InfluxDB tag value.
newtype TagValue = TagValue ByteArray

-- | InfluxDB field key.
newtype FieldKey = FieldKey ByteArray

-- | InfluxDB field value.
newtype FieldValue = FieldValue ByteArray

-- | Tags and fields for a data point.
data Tags = Tags 
  -- Invariants: Tag keys and tag values are represented
  -- as a structure of arrays. These must agree in length.
  -- The information is represented this way to optimize for
  -- the common case in which the tags keys are constant.
  !(UnliftedArray TagKey)
  !(UnliftedArray TagValue)

data Fields = Fields
  -- There must be at least one field. InfluxDB requires
  -- this, so this client does too.
  !(UnliftedArray FieldKey) 
  !(UnliftedArray FieldValue)

data Point = Point
  { measurement :: !Measurement
  , tags :: !Tags
  , fields :: !Fields
  , time :: !Word64
  }

coerceTagKeys :: UnliftedArray TagKey -> UnliftedArray ByteArray
coerceTagKeys (UnliftedArray x) = UnliftedArray x

coerceTagValues :: UnliftedArray TagValue -> UnliftedArray ByteArray
coerceTagValues (UnliftedArray x) = UnliftedArray x

coerceFieldKeys :: UnliftedArray FieldKey -> UnliftedArray ByteArray
coerceFieldKeys (UnliftedArray x) = UnliftedArray x

coerceFieldValues :: UnliftedArray FieldValue -> UnliftedArray ByteArray
coerceFieldValues (UnliftedArray x) = UnliftedArray x

c2w :: Char -> Word8
{-# inline c2w #-}
c2w = fromIntegral . ord

-- Returns a positive number with the new index if data was
-- successfully written. Otherwise, returns the negation of
-- the length required to paste this metadata.
encodePoint :: Point -> BB.Builder
encodePoint (Point (Measurement msrmnt) (Tags tks tvs) (Fields fks fvs) time) =
  BB.construct $ \(MutableBytes arr off len) -> do
    let !requiredBytes = 0
          + PM.sizeofByteArray msrmnt -- measurement
          + PM.sizeofUnliftedArray tks -- commas for tags
          + sumSizeofByteArrays (coerceTagKeys tks) -- tag keys
          + sumSizeofByteArrays (coerceTagValues tvs) -- tag values
          + PM.sizeofUnliftedArray fks -- commas (and space) for tags
          + sumSizeofByteArrays (coerceFieldKeys fks) -- field keys
          + sumSizeofByteArrays (coerceFieldValues fvs) -- field values
          + ( 1 -- space preceeding timestamp
            + 19 -- decimal-encoded timestamp
            + 1 -- newline after timestamp
            )
    sz <- PM.getSizeofMutableByteArray arr
    if requiredBytes <= len
      then do
        let i0 = 0
        i1 <- copySmall arr i0 msrmnt
        i2 <- copyTags arr i1 tks tvs
        -- Passing a space to copyFields since that
        -- is the initial separator that gets used.
        i3 <- copyFields arr i2 fks fvs (c2w ' ')
        -- Write the space after fields.
        PM.writeByteArray arr i3 (c2w ' ')
        let i4 = i3 + 1
        i5 <- BBU.pasteST (BBU.word64Dec time) arr i4
        -- Write the newline after fields.
        PM.writeByteArray arr i5 (c2w '\n')
        let i6 = i5 + 1
        pure (Just i6)
      else pure Nothing

sumSizeofByteArrays :: UnliftedArray ByteArray -> Int
sumSizeofByteArrays arr =
  let go !acc !ix = if ix >= 0
        then go (acc + PM.sizeofByteArray (PM.indexUnliftedArray arr ix)) (ix - 1)
        else acc
   in go 0 (PM.sizeofUnliftedArray arr - 1)

-- Precondition: there must be enough space in the
-- destination buffer.
copySmall ::
     MutableByteArray s
  -> Int
  -> ByteArray
  -> ST s Int
{-# inline copySmall #-}
copySmall dst doff0 src =
  go doff0 0 (PM.sizeofByteArray src)
  where
  go doff soff len = if soff < len
    then do
      PM.writeByteArray dst doff (PM.indexByteArray src soff :: Word8)
      go (doff + 1) (soff + 1) len
    else pure doff

copyTags ::
     MutableByteArray s -- dest
  -> Int -- dest index
  -> UnliftedArray TagKey
  -> UnliftedArray TagValue
  -> ST s Int
copyTags dst i' keys vals = go 0 i' where
  !tagSz = PM.sizeofUnliftedArray keys
  go !tagIx !i0 = if tagIx < tagSz
    then do
      let TagKey key = PM.indexUnliftedArray keys tagIx
      let TagValue val = PM.indexUnliftedArray vals tagIx
      -- Write the comma.
      PM.writeByteArray dst i0 (0x2C :: Word8)
      let i1 = i0 + 1
      i2 <- copySmall dst i1 key
      -- Write the equals sign.
      PM.writeByteArray dst i2 (0x3D :: Word8)
      let i3 = i2 + 1
      i4 <- copySmall dst i3 val
      go (tagIx + 1) i4 
    else pure i0

copyFields :: forall s.
     MutableByteArray s -- dest
  -> Int -- dest index
  -> UnliftedArray FieldKey
  -> UnliftedArray FieldValue
  -> Word8 -- separator
  -> ST s Int
copyFields dst i' keys vals = go 0 i' where
  !tagSz = PM.sizeofUnliftedArray keys
  go :: Int -> Int -> Word8 -> ST s Int
  go !tagIx !i0 !sep = if tagIx < tagSz
    then do
      let FieldKey key = PM.indexUnliftedArray keys tagIx
      let FieldValue val = PM.indexUnliftedArray vals tagIx
      -- Write the comma.
      PM.writeByteArray dst i0 sep
      let i1 = i0 + 1
      i2 <- copySmall dst i1 key
      -- Write the equals sign.
      PM.writeByteArray dst i2 (0x3D :: Word8)
      let i3 = i2 + 1
      i4 <- copySmall dst i3 val
      go (tagIx + 1) i4 0x2C -- use comma as separator
    else pure i0

escapeMeasurement :: ByteArray -> Measurement
-- TODO: escape this
escapeMeasurement = Measurement

tagKey :: ByteArray -> TagKey
-- TODO: escape this
tagKey = TagKey

tagValue :: ByteArray -> TagValue
-- TODO: escape this
tagValue = TagValue

fieldKey :: ByteArray -> FieldKey
-- TODO: escape this
fieldKey = FieldKey

fieldValue :: ByteArray -> FieldValue
-- TODO: escape this
fieldValue = FieldValue

fieldValueWord64 :: Word64 -> FieldValue
fieldValueWord64 = FieldValue . BBU.run . BBU.word64Dec

tags1 :: TagKey -> TagValue -> Tags
tags1 k0 =
  let !ks = runST $ do
        a <- PM.unsafeNewUnliftedArray 1
        PM.writeUnliftedArray a 0 k0
        PM.unsafeFreezeUnliftedArray a
   in \v0 ->
      let !vs = runST $ do
            a <- PM.unsafeNewUnliftedArray 1
            PM.writeUnliftedArray a 0 v0
            PM.unsafeFreezeUnliftedArray a
       in Tags ks vs

fields1 :: FieldKey -> FieldValue -> Fields
fields1 k0 =
  let !ks = runST $ do
        a <- PM.unsafeNewUnliftedArray 1
        PM.writeUnliftedArray a 0 k0
        PM.unsafeFreezeUnliftedArray a
   in \v0 ->
      let !vs = runST $ do
            a <- PM.unsafeNewUnliftedArray 1
            PM.writeUnliftedArray a 0 v0
            PM.unsafeFreezeUnliftedArray a
       in Fields ks vs
