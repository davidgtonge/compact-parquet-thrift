use compact_parquet_thrift::{Error, Reader, SliceSink, WireType as T, Writer};

const REFERENCE: &[u8] = include_bytes!("fixtures/compact_reference.bin");
const PAGE_HEADER: &[u8] = include_bytes!("fixtures/page_header_reference.bin");

#[test]
fn reads_python_apache_thrift_reference() {
    let mut r = Reader::new(REFERENCE);
    let mut root = r.structure().unwrap();

    let f = root.next_field().unwrap().unwrap();
    assert_eq!((f.id, f.kind), (1, T::I32));
    assert_eq!(root.i32().unwrap(), 123);

    let f = root.next_field().unwrap().unwrap();
    assert_eq!(f.id, 2);
    assert!(f.bool_value().unwrap());

    let f = root.next_field().unwrap().unwrap();
    assert_eq!((f.id, f.kind), (5, T::List));
    let list = root.list_header().unwrap();
    assert_eq!((list.element, list.len), (T::Binary, 2));
    assert_eq!(root.binary().unwrap(), b"UK");
    assert_eq!(root.binary().unwrap(), b"FR");

    let f = root.next_field().unwrap().unwrap();
    assert_eq!((f.id, f.kind), (20, T::Struct));
    let mut inner = root.structure().unwrap();
    let f = inner.next_field().unwrap().unwrap();
    assert_eq!((f.id, f.kind), (1, T::I64));
    assert_eq!(inner.i64().unwrap(), -5);
    assert_eq!(inner.next_field().unwrap(), None);
    drop(inner);

    let f = root.next_field().unwrap().unwrap();
    assert_eq!((f.id, f.kind), (30, T::Map));
    let map = root.map_header().unwrap();
    assert_eq!(map.len, 2);
    assert_eq!(map.types, Some((T::I16, T::BoolTrue)));
    assert_eq!(root.i16().unwrap(), 7);
    assert!(!root.bool_element().unwrap());
    assert_eq!(root.i16().unwrap(), -2);
    assert!(root.bool_element().unwrap());
    assert_eq!(root.next_field().unwrap(), None);
    drop(root);
    assert_eq!(r.position(), REFERENCE.len());
}

#[test]
fn writer_matches_python_apache_thrift_reference() {
    let mut output = [0u8; 128];
    let mut w = Writer::new(SliceSink::new(&mut output));
    w.structure(|w| {
        w.field_i32(1, 123)?;
        w.field_bool(2, true)?;
        w.field_header(5, T::List)?;
        w.list_header(T::Binary, 2)?;
        w.binary(b"UK")?;
        w.binary(b"FR")?;
        w.field_struct(20, |w| w.field_i64(1, -5))?;
        w.field_header(30, T::Map)?;
        w.map_header(T::I16, T::BoolTrue, 2)?;
        w.i16(7)?;
        w.bool_element(false)?;
        w.i16(-2)?;
        w.bool_element(true)?;
        Ok(())
    })
    .unwrap();
    assert_eq!(w.into_inner().as_written(), REFERENCE);
}

#[test]
fn raw_fields_can_be_reemitted_losslessly() {
    let mut r = Reader::new(REFERENCE);
    let mut output = [0u8; 128];
    let mut w = Writer::new(SliceSink::new(&mut output));
    w.structure(|w| {
        let mut s = r.structure()?;
        while let Some(f) = s.next_field()? {
            let raw = s.raw_value(f.kind)?;
            w.raw_field(f.id, f.kind, raw)?;
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(w.into_inner().as_written(), REFERENCE);
}

#[test]
fn long_list_and_out_of_order_field_ids() {
    let mut output = [0u8; 512];
    let mut w = Writer::new(SliceSink::new(&mut output));
    w.structure(|w| {
        w.field_header(100, T::List)?;
        w.list_header(T::I16, 16)?;
        for i in -8..8 {
            w.i16(i)?;
        }
        w.field_i32(-3, i32::MIN)?;
        w.field_i64(2, i64::MIN)?;
        w.field_double(3, 12.5)?;
        Ok(())
    })
    .unwrap();
    let written = w.into_inner();
    let mut r = Reader::new(written.as_written());
    let mut s = r.structure().unwrap();
    let f = s.next_field().unwrap().unwrap();
    assert_eq!((f.id, f.kind), (100, T::List));
    let list = s.list_header().unwrap();
    assert_eq!(list.len, 16);
    for i in -8..8 {
        assert_eq!(s.i16().unwrap(), i);
    }
    assert_eq!(s.next_field().unwrap().unwrap().id, -3);
    assert_eq!(s.i32().unwrap(), i32::MIN);
    assert_eq!(s.next_field().unwrap().unwrap().id, 2);
    assert_eq!(s.i64().unwrap(), i64::MIN);
    assert_eq!(s.next_field().unwrap().unwrap().id, 3);
    assert_eq!(s.f64().unwrap(), 12.5);
    assert_eq!(s.next_field().unwrap(), None);
}

#[test]
fn set_position_rewinds_and_rejects_past_the_end() {
    let input = [3, b'a', b'b', b'c'];
    let mut r = Reader::new(&input);
    assert_eq!(r.binary().unwrap(), b"abc");
    assert_eq!(r.remaining(), 0);
    r.set_position(0).unwrap();
    assert_eq!(r.binary().unwrap(), b"abc");
    assert_eq!(r.set_position(5), Err(Error::EndOfInput));
}

#[test]
fn binary_returns_a_borrow_of_original_input() {
    let input = [3, b'a', b'b', b'c'];
    let mut r = Reader::new(&input);
    let binary = r.binary().unwrap();
    assert_eq!(binary, b"abc");
    assert_eq!(binary.as_ptr(), input[1..].as_ptr());
}

#[test]
fn malformed_input_is_rejected() {
    assert_eq!(Reader::new(&[0x80]).var_u64(), Err(Error::EndOfInput));
    assert_eq!(
        Reader::new(&[0xff; 10]).var_u64(),
        Err(Error::InvalidVarint)
    );
    assert_eq!(
        Reader::new(&[0x80; 10]).var_u64(),
        Err(Error::InvalidVarint)
    );
    assert_eq!(
        Reader::new(&[0xff, 0xff, 0xff, 0xff, 0x1f]).var_u32(),
        Err(Error::IntegerOverflow)
    );
    assert_eq!(Reader::new(&[4, b'a']).binary(), Err(Error::EndOfInput));
    assert_eq!(
        Reader::new(&[0x1e]).structure().unwrap().next_field(),
        Err(Error::InvalidWireType(14))
    );
    assert_eq!(Reader::new(&[3]).bool_element(), Err(Error::InvalidBool(3)));
    assert_eq!(Reader::new(&[1, 0xff]).string(), Err(Error::InvalidUtf8));
}

#[test]
fn nested_skip_has_bounded_depth() {
    let mut r = Reader::with_max_depth(&[0x1c, 0x00, 0x00], 1);
    let mut top = r.structure().unwrap();
    let child = top.next_field().unwrap().unwrap();
    assert_eq!(top.skip(child.kind), Err(Error::DepthLimit));
}

#[test]
fn output_buffer_is_bounded() {
    let mut tiny = [0u8; 1];
    let mut w = Writer::new(SliceSink::new(&mut tiny));
    assert_eq!(
        w.structure(|w| w.field_binary(1, b"large")),
        Err(Error::OutputFull)
    );
}

#[test]
fn parquet_page_header_python_reference() {
    let (header, consumed) = compact_parquet_thrift::parquet::page_header(PAGE_HEADER).unwrap();
    assert_eq!(consumed, PAGE_HEADER.len());
    assert_eq!(header.page_type, Some(3));
    assert_eq!(header.uncompressed_size, Some(4096));
    assert_eq!(header.compressed_size, Some(1024));
    let v2 = header.data_v2.unwrap();
    assert_eq!(v2.num_values, Some(1000));
    assert_eq!(v2.num_nulls, Some(5));
    assert_eq!(v2.num_rows, Some(995));
    assert_eq!(v2.encoding, Some(8));
    assert_eq!(v2.definition_levels_bytes, Some(100));
    assert_eq!(v2.repetition_levels_bytes, Some(0));
    assert!(v2.is_compressed);
    assert_eq!(
        compact_parquet_thrift::parquet::page_header(&PAGE_HEADER[..5]),
        Err(Error::EndOfInput)
    );
}

#[test]
fn write_parquet_page_header_matches_reference() {
    let mut output = [0u8; 128];
    let mut w = Writer::new(SliceSink::new(&mut output));
    w.structure(|w| {
        w.field_i32(1, 3)?;
        w.field_i32(2, 4096)?;
        w.field_i32(3, 1024)?;
        w.field_struct(8, |w| {
            w.field_i32(1, 1000)?;
            w.field_i32(2, 5)?;
            w.field_i32(3, 995)?;
            w.field_i32(4, 8)?;
            w.field_i32(5, 100)?;
            w.field_i32(6, 0)?;
            w.field_bool(7, true)
        })
    })
    .unwrap();
    assert_eq!(w.into_inner().as_written(), PAGE_HEADER);
}

#[test]
fn skip_unknown_bool_collections_consumes_element_bytes() {
    let mut data = [0u8; 64];
    let mut w = Writer::new(SliceSink::new(&mut data));
    w.structure(|w| {
        w.field_header(1, T::List)?;
        w.list_header(T::BoolTrue, 3)?;
        w.bool_element(true)?;
        w.bool_element(false)?;
        w.bool_element(true)?;
        w.field_i32(2, 44)
    })
    .unwrap();
    let output = w.into_inner();
    let mut r = Reader::new(output.as_written());
    let mut s = r.structure().unwrap();
    let f = s.next_field().unwrap().unwrap();
    s.skip(f.kind).unwrap();
    let f = s.next_field().unwrap().unwrap();
    assert_eq!(f.id, 2);
    assert_eq!(s.i32().unwrap(), 44);
    assert_eq!(s.next_field().unwrap(), None);
}
