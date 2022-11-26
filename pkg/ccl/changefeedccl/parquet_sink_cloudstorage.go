// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	pqexporter "github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	parquetcommon "github.com/xitongsys/parquet-go/common"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// This variable controls whether we add primary keys of the table to the
// metadata of the parquet file. Currently, this will be true only under
// testing.
// TODO(cdc): We should consider including this metadata during production also
var includeParquetTestMetadata = false

// This is an extra column that will be added to every parquet file which tells
// us about the type of event that generated a particular row. The types are
// defined below.
const parquetCrdbEventTypeColName string = "__crdb_event_type__"

const (
	parquetEventInsert string = "c"
	parquetEventUpdate string = "u"
	parquetEventDelete string = "d"
)

// We need a separate sink for parquet format because the parquet encoder has to
// write metadata to the parquet file (buffer) after each flush. This means that the
// parquet encoder should have access to the buffer object inside
// cloudStorageSinkFile file. This means that the parquet writer has to be
// embedded in the cloudStorageSinkFile file. If we wanted to maintain the
// existing separation between encoder and the sync, then we would need to
// figure out a way to get the embedded parquet writer in the
// cloudStorageSinkFile and pass it to the encode function in the encoder.
// Instead of this it logically made sense to have a single sink for parquet
// format which did the job of both encoding and emitting to the cloud storage.
// This sink currently embeds the cloudStorageSinkFile and it has a function
// EncodeAndEmitRow which links the buffer in the cloudStorageSinkFile and the
// parquetWriter every time we need to create a file for each unique combination
// of topic and schema version (We need to have a unique parquetWriter for each
// unique combination of topic and schema version because each parquet file can
// have a single schema written inside it and each parquetWriter can only be
// associated with a single schema.)
type parquetCloudStorageSink struct {
	wrapped     *cloudStorageSink
	compression parquet.CompressionCodec
}

type parquetFileWriter struct {
	parquetWriter  *writer.ParquetWriter
	parquetColumns []pqexporter.ParquetColumn
	metadata       map[string]string
	numCols        int
}

func makeParquetCloudStorageSink(
	baseCloudStorageSink *cloudStorageSink,
) (*parquetCloudStorageSink, error) {
	parquetSink := &parquetCloudStorageSink{wrapped: baseCloudStorageSink}
	if !baseCloudStorageSink.compression.enabled() {
		parquetSink.compression = parquet.CompressionCodec_UNCOMPRESSED
	} else if baseCloudStorageSink.compression == sinkCompressionGzip {
		parquetSink.compression = parquet.CompressionCodec_GZIP
	} else {
		return nil, errors.AssertionFailedf("Specified compression not supported with parquet")
	}
	return parquetSink, nil
}

// EmitRow does not do anything. It must not be called. It is present so that
// parquetCloudStorageSink implements the Sink interface.
func (parquetSink *parquetCloudStorageSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	return errors.AssertionFailedf("Emit Row should not be called for parquet format")
}

// Close implements the Sink interface.
func (parquetSink *parquetCloudStorageSink) Close() error {
	return parquetSink.wrapped.Close()
}

// Dial implements the Sink interface.
func (parquetSink *parquetCloudStorageSink) Dial() error {
	return parquetSink.wrapped.Dial()
}

// EmitResolvedTimestamp does not do anything as of now. It is there to
// implement Sink interface.
func (parquetSink *parquetCloudStorageSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	return errors.AssertionFailedf("Parquet format does not support emitting resolved timestamp")
}

// Flush implements the Sink interface.
func (parquetSink *parquetCloudStorageSink) Flush(ctx context.Context) error {
	return parquetSink.wrapped.Flush(ctx)
}

// EncodeAndEmitRow links the buffer in the cloud storage sync file and the
// parquet writer (see parquetCloudStorageSink). It also takes care of encoding
// and emitting row event to cloud storage. Implements the SinkWithEncoder
// interface.
func (parquetSink *parquetCloudStorageSink) EncodeAndEmitRow(
	ctx context.Context,
	updatedRow cdcevent.Row,
	prevRow cdcevent.Row,
	topic TopicDescriptor,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	s := parquetSink.wrapped
	file, err := s.getOrCreateFile(topic, mvcc)
	if err != nil {
		return err
	}
	file.alloc.Merge(&alloc)

	if file.parquetCodec == nil {
		var err error
		file.parquetCodec, err = makeParquetWriterWrapper(ctx, updatedRow, &file.buf, parquetSink.compression)
		if err != nil {
			return err
		}
	}

	colOrd := -1
	// TODO (ganeshb): Avoid map allocation on every call to emit row
	parquetRow := make(map[string]interface{}, file.parquetCodec.numCols)
	if err := updatedRow.ForAllColumns().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		colOrd++
		// Omit NULL columns from parquet row
		if d == tree.DNull {
			parquetRow[col.Name] = nil
			return nil
		}
		encodeFn, err := file.parquetCodec.parquetColumns[colOrd].GetEncoder()
		if err != nil {
			return err
		}
		edNative, err := encodeFn(d)
		if err != nil {
			return err
		}

		parquetRow[col.Name] = edNative

		return nil

	}); err != nil {
		return err
	}

	if updatedRow.IsDeleted() {
		parquetRow[parquetCrdbEventTypeColName] = []byte(parquetEventDelete)
	} else if prevRow.IsInitialized() && !prevRow.IsDeleted() {
		parquetRow[parquetCrdbEventTypeColName] = []byte(parquetEventUpdate)
	} else {
		parquetRow[parquetCrdbEventTypeColName] = []byte(parquetEventInsert)
	}

	if err = file.parquetCodec.parquetWriter.AddData(parquetRow); err != nil {
		return err
	}

	if file.parquetCodec.parquetWriter.CurrentRowGroupSize() > s.targetMaxFileSize {
		s.metrics.recordSizeBasedFlush()

		if err = file.parquetCodec.parquetWriter.Close(); err != nil {
			return err
		}
		if err := s.flushTopicVersions(ctx, file.topic, file.schemaID); err != nil {
			return err
		}
	}

	return nil
}

func makeParquetWriterWrapper(
	ctx context.Context, row cdcevent.Row, buf *bytes.Buffer, compression parquet.CompressionCodec,
) (*parquetFileWriter, error) {
	parquetColumns, schemaElements, err := getParquetColumnTypes(ctx, row)
	if err != nil {
		return nil, err
	}

	pqw, err := writer.NewParquetWriterFromWriter(buf, schemaElements, 1)
	if err != nil {
		return nil, err
	}
	// TODO(cdc): Determine if we should parquet's builtin compressor or rely on
	// sinks compressing. Currently using not parquets builtin compressor, relying
	// on sinks compression
	pqw.CompressionType = compression

	pqww := &parquetFileWriter{}
	pqww.parquetWriter = pqw
	pqww.parquetColumns = parquetColumns
	pqww.numCols = len(parquetColumns)

	// TODO(cdc): We really should revisit if we should include any metadata in
	// parquet files. There are plenty things we can include there, including crdb
	// native column types, OIDs for those column types, etc
	if includeParquetTestMetadata {
		metadata, err := getMetadataForParquetFile(ctx, row)
		if err != nil {
			return nil, err
		}
		pqww.metadata = metadata
	}

	return pqww, nil
}

func getMetadataForParquetFile(ctx context.Context, row cdcevent.Row) (map[string]string, error) {
	metadata := make(map[string]string)
	primaryKeyColNames := ""
	columnNames := ""
	if err := row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		primaryKeyColNames += col.Name + ","
		return nil
	}); err != nil {
		return nil, err
	}
	metadata["primaryKeyNames"] = primaryKeyColNames
	if err := row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		columnNames += col.Name + ","
		return nil
	}); err != nil {
		return nil, err
	}
	metadata["columnNames"] = columnNames
	return metadata, nil
}
func encodeTag(tag *parquetcommon.Tag) (string, error) {
	v := reflect.ValueOf(*tag)
	typeOfV := v.Type()
	encoded := ""

	for i := 0; i < v.NumField(); i++ {

		name := typeOfV.Field(i).Name
		val := v.Field(i).Interface()
		if !isEmptyValue(v.Field(i)) {
			if len(encoded) != 0 {
				encoded += ", "
			}
			encoded += name + "="
			encoded += fmt.Sprintf("%v", val)
		}
	}

	return encoded, nil
}
func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Pointer:
		return v.IsNil()
	}
	return false
}

//	func getSchemaElement(typ *types.T, name string, nullable bool) (*parquet.SchemaElement, error){
//		schemaEl := parquet.NewSchemaElement()
//		schemaEl.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_OPTIONAL)
//		if !nullable {
//			schemaEl.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.FieldRepetitionType_REQUIRED)
//		}
//		schemaEl.Name = name
//
//		switch typ.Family() {
//		case types.BoolFamily:
//			schemaEl.Type = parquet.TypePtr(parquet.Type_BOOLEAN)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				return bool(*d.(*tree.DBool)), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				return tree.MakeDBool(tree.DBool(x.(bool))), nil
//			}
//
//		case types.StringFamily:
//			populateLogicalStringCol(schemaEl)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				return []byte(*d.(*tree.DString)), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				return tree.NewDString(string(x.([]byte))), nil
//			}
//		case types.CollatedStringFamily:
//			populateLogicalStringCol(schemaEl)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				return []byte(d.(*tree.DCollatedString).Contents), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				return tree.NewDCollatedString(string(x.([]byte)), typ.Locale(), &tree.CollationEnvironment{})
//			}
//		case types.INetFamily:
//			populateLogicalStringCol(schemaEl)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				return []byte(d.(*tree.DIPAddr).IPAddr.String()), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				return tree.ParseDIPAddrFromINetString(string(x.([]byte)))
//			}
//		case types.JsonFamily:
//			schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
//			schemaEl.LogicalType = parquet.NewLogicalType()
//			schemaEl.LogicalType.JSON = parquet.NewJsonType()
//			schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_JSON)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				return []byte(d.(*tree.DJSON).JSON.String()), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				jsonStr := string(x.([]byte))
//				return tree.ParseDJSON(jsonStr)
//			}
//
//		case types.IntFamily:
//			schemaEl.LogicalType = parquet.NewLogicalType()
//			schemaEl.LogicalType.INTEGER = parquet.NewIntType()
//			schemaEl.LogicalType.INTEGER.IsSigned = true
//			if typ.Oid() == oid.T_int8 {
//				schemaEl.Type = parquet.TypePtr(parquet.Type_INT64)
//				schemaEl.LogicalType.INTEGER.BitWidth = int8(64)
//				schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_64)
//				col.encodeFn = func(d tree.Datum) (interface{}, error) {
//					return int64(*d.(*tree.DInt)), nil
//				}
//				col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//					return tree.NewDInt(tree.DInt(x.(int64))), nil
//				}
//			} else {
//				schemaEl.Type = parquet.TypePtr(parquet.Type_INT32)
//				schemaEl.LogicalType.INTEGER.BitWidth = int8(32)
//				schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_INT_32)
//				col.encodeFn = func(d tree.Datum) (interface{}, error) {
//					return int32(*d.(*tree.DInt)), nil
//				}
//				col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//					return tree.NewDInt(tree.DInt(x.(int32))), nil
//				}
//			}
//		case types.FloatFamily:
//			if typ.Oid() == oid.T_float4 {
//				schemaEl.Type = parquet.TypePtr(parquet.Type_FLOAT)
//				col.encodeFn = func(d tree.Datum) (interface{}, error) {
//					h := float32(*d.(*tree.DFloat))
//					return h, nil
//				}
//				col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//					// must convert float32 to string before converting to float64 (the
//					// underlying data type of a tree.Dfloat) because directly converting
//					// a float32 to a float64 will add on trailing significant digits,
//					// causing the round trip tests to fail.
//					hS := fmt.Sprintf("%f", x.(float32))
//					return tree.ParseDFloat(hS)
//				}
//			} else {
//				schemaEl.Type = parquet.TypePtr(parquet.Type_DOUBLE)
//				col.encodeFn = func(d tree.Datum) (interface{}, error) {
//					return float64(*d.(*tree.DFloat)), nil
//				}
//				col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//					return tree.NewDFloat(tree.DFloat(x.(float64))), nil
//				}
//			}
//		case types.DecimalFamily:
//			// TODO (MB): Investigate if the parquet vendor should enforce precision and
//			// scale requirements. In a toy example, the parquet vendor was able to
//			// write/read roundtrip the string "3235.5432" as a Decimal with Scale = 1,
//			// Precision = 1, even though this decimal has a larger scale and precision.
//			// I guess it's the responsibility of CRDB to enforce the Scale and
//			// Precision conditions, and for the parquet vendor to NOT lose data, even if
//			// the data doesn't follow the scale and precision conditions.
//
//			schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
//
//			schemaEl.LogicalType = parquet.NewLogicalType()
//			schemaEl.LogicalType.DECIMAL = parquet.NewDecimalType()
//
//			schemaEl.LogicalType.DECIMAL.Scale = typ.Scale()
//			schemaEl.LogicalType.DECIMAL.Precision = typ.Precision()
//			schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_DECIMAL)
//
//			// According to PostgresSQL docs, scale or precision of 0 implies max
//			// precision and scale. I assume this is what CRDB does, but this isn't
//			// explicit in the docs https://www.postgresql.org/docs/10/datatype-numeric.html
//			if typ.Scale() == 0 {
//				schemaEl.LogicalType.DECIMAL.Scale = math.MaxInt32
//			}
//			if typ.Precision() == 0 {
//				schemaEl.LogicalType.DECIMAL.Precision = math.MaxInt32
//			}
//
//			schemaEl.Scale = &schemaEl.LogicalType.DECIMAL.Scale
//			schemaEl.Precision = &schemaEl.LogicalType.DECIMAL.Precision
//
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				dec := d.(*tree.DDecimal).Decimal
//				return []byte(dec.String()), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				// TODO (MB): investigative if crdb should gather decimal metadata from
//				// parquet file during IMPORT PARQUET.
//				return tree.ParseDDecimal(string(x.([]byte)))
//			}
//		case types.UuidFamily:
//			// Vendor parquet documentation suggests that UUID maps to the [16]byte go type
//			// https://github.com/fraugster/parquet-go#supported-logical-types
//			schemaEl.Type = parquet.TypePtr(parquet.Type_FIXED_LEN_BYTE_ARRAY)
//			byteArraySize := int32(uuid.Size)
//			schemaEl.TypeLength = &byteArraySize
//			schemaEl.LogicalType = parquet.NewLogicalType()
//			schemaEl.LogicalType.UUID = parquet.NewUUIDType()
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				return d.(*tree.DUuid).UUID.GetBytes(), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				return tree.ParseDUuidFromBytes(x.([]byte))
//			}
//		case types.BytesFamily:
//			schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				return []byte(*d.(*tree.DBytes)), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				return tree.NewDBytes(tree.DBytes(x.([]byte))), nil
//			}
//		case types.BitFamily:
//			schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				// TODO(MB): investigate whether bit arrays should be encoded as an array of longs,
//				// like in avro changefeeds
//				baS := RoundtripStringer(d.(*tree.DBitArray))
//				return []byte(baS), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				ba, err := bitarray.Parse(string(x.([]byte)))
//				return &tree.DBitArray{BitArray: ba}, err
//			}
//		case types.EnumFamily:
//			schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
//			schemaEl.LogicalType = parquet.NewLogicalType()
//			schemaEl.LogicalType.ENUM = parquet.NewEnumType()
//			schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_ENUM)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				return []byte(d.(*tree.DEnum).LogicalRep), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				return tree.MakeDEnumFromLogicalRepresentation(typ, string(x.([]byte)))
//			}
//		case types.Box2DFamily:
//			populateLogicalStringCol(schemaEl)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				return []byte(d.(*tree.DBox2D).CartesianBoundingBox.Repr()), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				b, err := geo.ParseCartesianBoundingBox(string(x.([]byte)))
//				if err != nil {
//					return nil, err
//				}
//				return tree.NewDBox2D(b), nil
//			}
//		case types.GeographyFamily:
//			schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				return []byte(d.(*tree.DGeography).EWKB()), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				g, err := geo.ParseGeographyFromEWKB(geopb.EWKB(x.([]byte)))
//				if err != nil {
//					return nil, err
//				}
//				return &tree.DGeography{Geography: g}, nil
//			}
//		case types.GeometryFamily:
//			schemaEl.Type = parquet.TypePtr(parquet.Type_BYTE_ARRAY)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				return []byte(d.(*tree.DGeometry).EWKB()), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				g, err := geo.ParseGeometryFromEWKBUnsafe(geopb.EWKB(x.([]byte)))
//				if err != nil {
//					return nil, err
//				}
//				return &tree.DGeometry{Geometry: g}, nil
//			}
//		case types.DateFamily:
//			// Even though the parquet vendor supports Dates, we export Dates as strings
//			// because the vendor only supports encoding them as an int32, the Days
//			// since the Unix epoch, which according CRDB's `date.UnixEpochDays( )` (in
//			// pgdate package) is vulnerable to overflow.
//			populateLogicalStringCol(schemaEl)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				date := d.(*tree.DDate)
//				ds := RoundtripStringer(date)
//				return []byte(ds), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				dStr := string(x.([]byte))
//				d, dependCtx, err := tree.ParseDDate(nil, dStr)
//				if dependCtx {
//					return nil, errors.Newf("decoding date %s failed. depends on context", string(x.([]byte)))
//				}
//				return d, err
//			}
//		case types.TimeFamily:
//			schemaEl.Type = parquet.TypePtr(parquet.Type_INT64)
//			schemaEl.LogicalType = parquet.NewLogicalType()
//			schemaEl.LogicalType.TIME = parquet.NewTimeType()
//			t := parquet.NewTimeUnit()
//			t.MICROS = parquet.NewMicroSeconds()
//			schemaEl.LogicalType.TIME.Unit = t
//			schemaEl.LogicalType.TIME.IsAdjustedToUTC = true // per crdb docs
//			schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_TIME_MICROS)
//
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				// Time of day is stored in microseconds since midnight,
//				// which is also how parquet stores time
//				time := d.(*tree.DTime)
//				m := int64(*time)
//				return m, nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				return tree.MakeDTime(timeofday.TimeOfDay(x.(int64))), nil
//			}
//		case types.TimeTZFamily:
//			// The parquet vendor does not support an efficient encoding of TimeTZ
//			// (i.e. a datetime field and a timezone field), so we must fall back to
//			// encoding the whole TimeTZ as a string.
//			populateLogicalStringCol(schemaEl)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				return []byte(d.(*tree.DTimeTZ).TimeTZ.String()), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				d, dependsOnCtx, err := tree.ParseDTimeTZ(nil, string(x.([]byte)), time.Microsecond)
//				if dependsOnCtx {
//					return nil, errors.New("parsed time depends on context")
//				}
//				return d, err
//			}
//		case types.IntervalFamily:
//			// The parquet vendor only supports intervals as a parquet converted type,
//			// but converted types have been deprecated in the Apache Parquet format.
//			// https://github.com/fraugster/parquet-go#supported-converted-types
//			populateLogicalStringCol(schemaEl)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				return []byte(d.(*tree.DInterval).ValueAsISO8601String()), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				return tree.ParseDInterval(duration.IntervalStyle_ISO_8601, string(x.([]byte)))
//			}
//		case types.TimestampFamily:
//			// Didn't encode this as Microseconds since the unix epoch because of threat
//			// of overflow. See comment associated with time.Time.UnixMicro().
//			populateLogicalStringCol(schemaEl)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				ts := RoundtripStringer(d.(*tree.DTimestamp))
//				return []byte(ts), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				// return tree.MakeDTimestamp(time.UnixMicro(x.(int64)).UTC(), time.Microsecond)
//				dtStr := string(x.([]byte))
//				d, dependsOnCtx, err := tree.ParseDTimestamp(nil, dtStr, time.Microsecond)
//				if dependsOnCtx {
//					return nil, errors.New("TimestampTZ depends on context")
//				}
//				if err != nil {
//					return nil, err
//				}
//				// Converts the timezone from "loc(+0000)" to "UTC", which are equivalent,
//				// allowing roundtrip tests to pass.
//				d.Time = d.Time.UTC()
//				return d, nil
//			}
//
//		case types.TimestampTZFamily:
//			// Didn't encode this as Microseconds since the unix epoch because of threat
//			// of overflow. See comment associated with time.Time.UnixMicro().
//			populateLogicalStringCol(schemaEl)
//
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				ts := RoundtripStringer(d.(*tree.DTimestampTZ))
//				return []byte(ts), nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				dtStr := string(x.([]byte))
//				d, dependsOnCtx, err := tree.ParseDTimestampTZ(nil, dtStr, time.Microsecond)
//				if dependsOnCtx {
//					return nil, errors.New("TimestampTZ depends on context")
//				}
//				if err != nil {
//					return nil, err
//				}
//				// Converts the timezone from "loc(+0000)" to "UTC", which are equivalent,
//				// allowing tests to pass.
//				d.Time = d.Time.UTC()
//				return d, nil
//			}
//		case types.ArrayFamily:
//
//			// Define a list such that the parquet schema in json is:
//			/*
//				required group colName (LIST){ // parent
//					repeated group list { // child
//						required colType element; //grandChild
//					}
//				}
//			*/
//			// MB figured this out by running toy examples of the fraugster-parquet
//			// vendor repository for added context, checkout this issue
//			// https://github.com/fraugster/parquet-go/issues/18
//
//			// First, define the grandChild definition, the schema for the array value.
//			grandChild, err := NewParquetColumn(typ.ArrayContents(), "element", true)
//			if err != nil {
//				return col, err
//			}
//			// Next define the child definition, required by fraugster-parquet vendor library. Again,
//			// there's little documentation on this. MB figured this out using a debugger.
//			child := &parquetschema.ColumnDefinition{}
//			child.SchemaElement = parquet.NewSchemaElement()
//			child.SchemaElement.RepetitionType = parquet.FieldRepetitionTypePtr(parquet.
//				FieldRepetitionType_REPEATED)
//			child.SchemaElement.Name = "list"
//			child.Children = []*parquetschema.ColumnDefinition{grandChild.definition}
//			ngc := int32(len(child.Children))
//			child.SchemaElement.NumChildren = &ngc
//
//			// Finally, define the parent definition.
//			col.definition.Children = []*parquetschema.ColumnDefinition{child}
//			nc := int32(len(col.definition.Children))
//			child.SchemaElement.NumChildren = &nc
//			schemaEl.LogicalType = parquet.NewLogicalType()
//			schemaEl.LogicalType.LIST = parquet.NewListType()
//			schemaEl.ConvertedType = parquet.ConvertedTypePtr(parquet.ConvertedType_LIST)
//			col.encodeFn = func(d tree.Datum) (interface{}, error) {
//				datumArr := d.(*tree.DArray)
//				els := make([]map[string]interface{}, datumArr.Len())
//				for i, elt := range datumArr.Array {
//					var el interface{}
//					if elt.ResolvedType().Family() == types.UnknownFamily {
//						// skip encoding the datum
//					} else {
//						el, err = grandChild.encodeFn(elt)
//						if err != nil {
//							return col, err
//						}
//					}
//					els[i] = map[string]interface{}{"element": el}
//				}
//				encEl := map[string]interface{}{"list": els}
//				return encEl, nil
//			}
//			col.DecodeFn = func(x interface{}) (tree.Datum, error) {
//				// The parquet vendor decodes an array into the native go type
//				// map[string]interface{}, and the values of the array are stored in the
//				// "list" key of the map. "list" maps to an array of maps
//				// []map[string]interface{}, where the ith map contains a single key value
//				// pair. The key is always "element" and the value is the ith value in the
//				// array.
//
//				// If the array of maps only contains an empty map, the array is empty. This
//				// occurs IFF "element" is not in the map.
//
//				// NB: there's a bug in the fraugster-parquet vendor library around
//				// reading an ARRAY[NULL],
//				// https://github.com/fraugster/parquet-go/issues/60 I already verified
//				// that the vendor's parquet writer can write arrays with null values just
//				// fine, so EXPORT PARQUET is bug free; however this roundtrip test would
//				// fail. Ideally, once the bug gets fixed, ARRAY[NULL] will get read as
//				// the kvp {"element":interface{}} while ARRAY[] will continue to get read
//				// as an empty map.
//				datumArr := tree.NewDArray(typ.ArrayContents())
//				datumArr.Array = []tree.Datum{}
//
//				intermediate := x.(map[string]interface{})
//				vals := intermediate["list"].([]map[string]interface{})
//				if _, nonEmpty := vals[0]["element"]; !nonEmpty {
//					if len(vals) > 1 {
//						return nil, errors.New("array is empty, it shouldn't have a length greater than 1")
//					}
//				} else {
//					for _, elMap := range vals {
//						itemDatum, err := grandChild.DecodeFn(elMap["element"])
//						if err != nil {
//							return nil, err
//						}
//						err = datumArr.Append(itemDatum)
//						if err != nil {
//							return nil, err
//						}
//					}
//				}
//				return datumArr, nil
//			}
//		default:
//			return nil, errors.Errorf("parquet export does not support the %v type yet", typ.Family())
//		}
//
//
//		}
//
// }
func getParquetColumnTypes(
	ctx context.Context, row cdcevent.Row,
) ([]pqexporter.ParquetColumn, []*parquet.SchemaElement, error) {
	typs := make([]*types.T, 0)
	names := make([]string, 0)
	var err error

	if err := row.ForAllColumns().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		typs = append(typs, col.Typ)
		names = append(names, col.Name)
		return nil
	}); err != nil {
		return nil, nil, err
	}

	parquetColumns := make([]pqexporter.ParquetColumn, len(typs)+1)
	schemaElements := make([]*parquet.SchemaElement, len(typs)+1)
	const nullable = true

	for i := 0; i < len(typs); i++ {
		// Make every field optional, so that all schema evolutions for a table are
		// considered "backward compatible" by parquet. This means that the parquet
		// type doesn't mirror the column's nullability, but it makes it much easier
		// to work with long histories of table data afterward, especially for
		// things like loading into analytics databases.
		parquetCol, err := pqexporter.NewParquetColumn(typs[i], names[i], nullable)
		if err != nil {
			return nil, nil, err
		}
		parquetColumns[i] = parquetCol

		temp := parquet.NewSchemaElement()
		parquetcommon.DeepCopy(parquetCol.GetSchemaElement(), temp)

		schemaElements = append(schemaElements, temp)
	}

	// Add the extra column which will store the type of event that generated that
	// particular row.

	parquetColumns[len(typs)], err = pqexporter.NewParquetColumn(types.String, parquetCrdbEventTypeColName, false)
	if err != nil {
		return nil, nil, err
	}

	return parquetColumns, schemaElements, nil
}
