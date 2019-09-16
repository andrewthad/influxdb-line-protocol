{-# language BangPatterns #-}
{-# language DerivingStrategies #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language LambdaCase #-}
{-# language MagicHash #-}
{-# language PolyKinds #-}
{-# language RankNTypes #-}
{-# language ScopedTypeVariables #-}
{-# language StandaloneDeriving #-}
{-# language TypeApplications #-}
{-# language TypeFamilies #-}
{-# language TypeInType #-}
{-# language UnboxedTuples #-}

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
  , measurementByteArray
  , tagKeyByteArray
  , tagValueByteArray
  , fieldKeyByteArray
  , fieldValueByteArray
  , fieldValueWord64
  , fieldValueInt64
  , fieldValueDouble
  , fieldValueBool
    -- * Construct Tags
  , tags1
  , tags2
  , fields1
  , fields2
  , fields4
  ) where

import Control.Monad.ST (ST)
import Control.Monad.ST.Run (runByteArrayST,runUnliftedArrayST)
import Data.Coerce (coerce)
import Data.Char (ord)
import Data.Bytes.Types (MutableBytes(..))
import Data.Word (Word8,Word64)
import Data.Int (Int64)
import Data.Primitive (ByteArray(..),MutableByteArray(..))
import Data.Primitive.Unlifted.Array (UnliftedArray(..))
import Data.Primitive.Unlifted.Class (PrimUnlifted(..))
import GHC.Int (Int(I#))
import GHC.Exts (ByteArray#,State#)
import GHC.ST (ST(ST))

import qualified Arithmetic.Nat as Nat
import qualified Control.Monad.Primitive as PM
import qualified Data.Primitive as PM
import qualified Data.Primitive.Unlifted.Array as PM
import qualified Data.ByteArray.Builder as B
import qualified Data.ByteArray.Builder.Unsafe as BU
import qualified Data.ByteArray.Builder.Bounded as BB
import qualified Data.ByteArray.Builder.Bounded.Unsafe as BBU
import qualified Data.Vector.Primitive as PV
import qualified GHC.Exts as Exts

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

-- | Tags for a data point.
data Tags = Tags
  -- Invariants: Tag keys and tag values are represented
  -- as a structure of arrays. These must agree in length.
  -- The information is represented this way to optimize for
  -- the common case in which the tags keys are constant.
  !(UnliftedArray TagKey)
  !(UnliftedArray TagValue)

instance Semigroup Tags where
  Tags a1 b1 <> Tags a2 b2 = Tags (a1 <> a2) (b1 <> b2)

instance Monoid Tags where
  mempty = Tags mempty mempty

-- | Fields for a data point.
data Fields = Fields
  -- There must be at least one field. InfluxDB requires
  -- this, so this client does too.
  !(UnliftedArray FieldKey)
  !(UnliftedArray FieldValue)

-- | An InfluxDB data point. The include the measurement that
-- the data point belongs to, the tags, the fields, and the
-- timestamp.
data Point = Point
  { measurement :: {-# UNPACK #-} !Measurement
  , tags :: {-# UNPACK #-} !Tags
  , fields :: {-# UNPACK #-} !Fields
  , time :: {-# UNPACK #-} !Word64
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

unST :: ST s a -> State# s -> (# State# s, a #)
unST (ST x) = x

unsafeConstructBuilder :: (forall s. MutableBytes s -> ST s (Maybe Int)) -> B.Builder
unsafeConstructBuilder f = BU.Builder
  (\arr off len s0 -> case unST (f (MutableBytes (MutableByteArray arr) (I# off) (I# len))) s0 of
    (# s1, m #) -> case m of
      Nothing -> (# s1, (-1#) #)
      Just (I# r) -> (# s1, r #)
  )

-- Returns a positive number with the new index if data was
-- successfully written. Otherwise, returns the negation of
-- the length required to paste this metadata.
encodePoint :: Point -> B.Builder
encodePoint (Point (Measurement msrmnt) (Tags tks tvs) (Fields fks fvs) theTime) =
  unsafeConstructBuilder $ \(MutableBytes arr off len) -> do
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
    if requiredBytes <= len
      then do
        let i0 = off
        i1 <- copySmall arr i0 msrmnt
        i2 <- copyTags arr i1 tks tvs
        -- Passing a space to copyFields since that
        -- is the initial separator that gets used.
        i3 <- copyFields arr i2 fks fvs (c2w ' ')
        -- Write the space after fields.
        PM.writeByteArray arr i3 (c2w ' ')
        let i4 = i3 + 1
        i5 <- BBU.pasteST (BB.word64Dec theTime) arr i4
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
-- destination buffer. I (Andrew) originally thought
-- that an inlined loop would outperform memcpy for
-- byte arrays that were known to be small (under 20 bytes).
-- However, it does not.
copySmall ::
     MutableByteArray s
  -> Int
  -> ByteArray
  -> ST s Int
{-# inline copySmall #-}
copySmall dst doff0 src = do
  let sz = PM.sizeofByteArray src
  PM.copyByteArray dst doff0 src 0 sz
  pure (doff0 + sz)

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

unByteArray :: ByteArray -> ByteArray#
unByteArray (ByteArray x) = x

escapeQuoted :: ByteArray# -> ByteArray#
{-# inline escapeQuoted #-}
escapeQuoted b = unByteArray $ runByteArrayST $ do
  r <- PM.newByteArray ((sz * 2) + 2)
  PM.writeByteArray r 0 (c2w '"')
  let go !ixSrc !ixDst = if ixSrc < sz
        then do
          let w :: Word8 = PM.indexByteArray (ByteArray b) ixSrc
          if w == c2w '\\' || w == c2w '"'
            then do
              PM.writeByteArray r ixDst (c2w '\\')
              PM.writeByteArray r (ixDst + 1) w
              go (ixSrc + 1) (ixDst + 2)
            else do
              PM.writeByteArray r ixDst w
              go (ixSrc + 1) (ixDst + 1)
        else pure ixDst
  len <- go 0 1
  PM.writeByteArray r len (c2w '"')
  shrinkMutableByteArray r (len + 1)
  PM.unsafeFreezeByteArray r
  where
  !sz = PM.sizeofByteArray (ByteArray b)

escapeCommon :: (Word8 -> Bool) -> ByteArray# -> ByteArray#
{-# inline escapeCommon #-}
escapeCommon p b = case m of
  Nothing -> b
  Just ix0 -> unByteArray $ runByteArrayST $ do
    r <- PM.newByteArray (ix0 + ((sz - ix0) * 2))
    PM.copyByteArray r 0 (ByteArray b) 0 ix0
    let go !ixSrc !ixDst = if ixSrc < sz
          then do
            let w :: Word8 = PM.indexByteArray (ByteArray b) ixSrc
            if p w
              then do
                PM.writeByteArray r ixDst (c2w '\\')
                PM.writeByteArray r (ixDst + 1) w
                go (ixSrc + 1) (ixDst + 2)
              else do
                PM.writeByteArray r ixDst w
                go (ixSrc + 1) (ixDst + 1)
          else pure ixDst
    len <- go ix0 ix0
    shrinkMutableByteArray r len
    PM.unsafeFreezeByteArray r
  where
  sz = PM.sizeofByteArray (ByteArray b)
  m = PV.findIndex p (PV.Vector 0 sz (ByteArray b))

escapeCommaSpaceEquals :: ByteArray# -> ByteArray#
{-# noinline escapeCommaSpaceEquals #-}
escapeCommaSpaceEquals b = escapeCommon (\c -> c == c2w ',' || c == c2w ' ' || c == c2w '=') b

-- This does not do any escaping of bytes that are outside
-- of the ASCII plane. It leaves them alone.
escapeCommaSpace :: ByteArray# -> ByteArray#
{-# noinline escapeCommaSpace #-}
escapeCommaSpace b = escapeCommon (\c -> c == c2w ',' || c == c2w ' ') b

shrinkMutableByteArray ::
     MutableByteArray s
  -> Int -- ^ new size
  -> ST s ()
{-# INLINE shrinkMutableByteArray #-}
shrinkMutableByteArray (PM.MutableByteArray arr#) (I# n#)
  = PM.primitive_ (Exts.shrinkMutableByteArray# arr# n#)

-- | Adds backslash before commas and spaces.
measurementByteArray :: ByteArray -> Measurement
{-# inline measurementByteArray #-}
measurementByteArray (ByteArray a) =
  Measurement (ByteArray (escapeCommaSpace a))

-- | Convert a 'ByteArray' to a field key by escaping commas, spaces
-- and the equals symbol. The argument must be be UTF-8 encoded text.
tagKeyByteArray :: ByteArray -> TagKey
tagKeyByteArray (ByteArray a) = TagKey (ByteArray (escapeCommaSpaceEquals a))

-- | Convert a 'ByteArray' to a field values by escaping commas, spaces
-- and the equals symbol. The argument must be be UTF-8 encoded text.
tagValueByteArray :: ByteArray -> TagValue
tagValueByteArray (ByteArray a) = TagValue (ByteArray (escapeCommaSpaceEquals a))

-- | Convert a 'ByteArray' to a tag key by escaping commas, spaces
-- and the equals symbol. The argument must be be UTF-8 encoded text.
fieldKeyByteArray :: ByteArray -> FieldKey
fieldKeyByteArray (ByteArray a) = FieldKey (ByteArray (escapeCommaSpaceEquals a))

-- | Convert a 'ByteArray' to a field value by wrapping it in
-- double quotes and backslash-escaping double quotes and backslashes.
-- The argument must be be UTF-8 encoded text.
fieldValueByteArray :: ByteArray -> FieldValue
fieldValueByteArray (ByteArray a) = FieldValue (ByteArray (escapeQuoted a))

-- | Convert a 'Word64' to a field value.
fieldValueWord64 :: Word64 -> FieldValue
fieldValueWord64 = FieldValue . BB.run Nat.constant . BB.word64Dec

-- | Convert a 'Int64' to a field value.
fieldValueInt64 :: Int64 -> FieldValue
fieldValueInt64 = FieldValue . BB.run Nat.constant . BB.int64Dec

-- | Convert a 'Double' to a field value.
fieldValueDouble :: Double -> FieldValue
fieldValueDouble = FieldValue . BB.run Nat.constant . BB.doubleDec

-- | Convert a 'Bool' to a field value.
fieldValueBool :: Bool -> FieldValue
fieldValueBool = \case
  True -> FieldValue trueByteArray
  False -> FieldValue falseByteArray

trueByteArray :: ByteArray
trueByteArray = runByteArrayST $ do
  a <- PM.newByteArray 1
  PM.writeByteArray a 0 (c2w 'T')
  PM.unsafeFreezeByteArray a

falseByteArray :: ByteArray
falseByteArray = runByteArrayST $ do
  a <- PM.newByteArray 1
  PM.writeByteArray a 0 (c2w 'F')
  PM.unsafeFreezeByteArray a

tags1 :: TagKey -> TagValue -> Tags
{-# inline tags1 #-}
tags1 !k0 =
  let !ks = runUnliftedArrayST $ do
        a <- PM.unsafeNewUnliftedArray 1
        PM.writeUnliftedArray a 0 k0
        PM.unsafeFreezeUnliftedArray a
   in \v0 ->
      let !vs = runUnliftedArrayST $ do
            a <- PM.unsafeNewUnliftedArray 1
            PM.writeUnliftedArray a 0 v0
            PM.unsafeFreezeUnliftedArray a
       in Tags ks vs

tags2 :: TagKey -> TagKey -> TagValue -> TagValue -> Tags
{-# inline tags2 #-}
tags2 !k0 !k1 =
  let ks = runUnliftedArrayST $ do
        a <- PM.unsafeNewUnliftedArray 2
        PM.writeUnliftedArray a 0 k0
        PM.writeUnliftedArray a 1 k1
        PM.unsafeFreezeUnliftedArray a
   in \v0 v1 ->
      let !vs = runUnliftedArrayST $ do
            a <- PM.unsafeNewUnliftedArray 2
            PM.writeUnliftedArray a 0 v0
            PM.writeUnliftedArray a 1 v1
            PM.unsafeFreezeUnliftedArray a
       in Tags ks vs

fields1 :: FieldKey -> FieldValue -> Fields
{-# inline fields1 #-}
fields1 !k0 =
  let ks = runUnliftedArrayST $ do
        a <- PM.unsafeNewUnliftedArray 1
        PM.writeUnliftedArray a 0 k0
        PM.unsafeFreezeUnliftedArray a
   in \v0 ->
      let !vs = runUnliftedArrayST $ do
            a <- PM.unsafeNewUnliftedArray 1
            PM.writeUnliftedArray a 0 v0
            PM.unsafeFreezeUnliftedArray a
       in Fields ks vs

fields2 :: FieldKey -> FieldKey -> FieldValue -> FieldValue -> Fields
{-# inline fields2 #-}
fields2 !k0 !k1 =
  -- If we put a bang pattern on ks, GHC decides not to float it
  -- to the top level when we use fields2 to make a template.
  -- We really really want it to get floated.
  let ks = runUnliftedArrayST $ do
        a <- PM.unsafeNewUnliftedArray 2
        PM.writeUnliftedArray a 0 k0
        PM.writeUnliftedArray a 1 k1
        PM.unsafeFreezeUnliftedArray a
   in \v0 v1 ->
      let !vs = runUnliftedArrayST $ do
            a <- PM.unsafeNewUnliftedArray 2
            PM.writeUnliftedArray a 0 v0
            PM.writeUnliftedArray a 1 v1
            PM.unsafeFreezeUnliftedArray a
       in Fields ks vs

fields4 :: FieldKey -> FieldKey -> FieldKey -> FieldKey -> FieldValue -> FieldValue -> FieldValue -> FieldValue -> Fields
{-# inline fields4 #-}
fields4 !k0 !k1 !k2 !k3 =
  -- If we put a bang pattern on ks, GHC decides not to float it
  -- to the top level when we use fields2 to make a template.
  -- We really really want it to get floated.
  let ks = runUnliftedArrayST $ do
        a <- PM.unsafeNewUnliftedArray 4
        PM.writeUnliftedArray a 0 k0
        PM.writeUnliftedArray a 1 k1
        PM.writeUnliftedArray a 2 k2
        PM.writeUnliftedArray a 3 k3
        PM.unsafeFreezeUnliftedArray a
   in \v0 v1 v2 v3 ->
      let !vs = runUnliftedArrayST $ do
            a <- PM.unsafeNewUnliftedArray 4
            PM.writeUnliftedArray a 0 v0
            PM.writeUnliftedArray a 1 v1
            PM.writeUnliftedArray a 2 v2
            PM.writeUnliftedArray a 3 v3
            PM.unsafeFreezeUnliftedArray a
       in Fields ks vs
