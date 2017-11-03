// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use kvproto::coprocessor::KeyRange;
use tipb::executor::TableScan;

use util::collections::HashSet;
use storage::{SnapshotStore, Statistics};
use coprocessor::codec::table;
use coprocessor::endpoint::{is_point, prefix_next};
use coprocessor::Result;
use coprocessor::metrics::*;

use super::{Executor, Row};
use super::scanner::Scanner;


pub struct TableScanExecutor<'a> {
    desc: bool,
    col_ids: HashSet<i64>,
    cursor: usize,
    key_ranges: Vec<KeyRange>,
    scanner: Scanner<'a>,
}

impl<'a> TableScanExecutor<'a> {
    pub fn new(
        meta: &TableScan,
        mut key_ranges: Vec<KeyRange>,
        store: SnapshotStore<'a>,
        statistics: &'a mut Statistics,
    ) -> TableScanExecutor<'a> {
        let col_ids = meta.get_columns()
            .iter()
            .filter(|c| !c.get_pk_handle())
            .map(|c| c.get_column_id())
            .collect();
        let desc = meta.get_desc();
        if desc {
            key_ranges.reverse();
        }
        let scanner = Scanner::new(store, desc, false, statistics);
        COPR_EXECUTOR_COUNT.with_label_values(&["tblscan"]).inc();
        TableScanExecutor {
            desc: desc,
            col_ids: col_ids,
            scanner: scanner,
            key_ranges: key_ranges,
            cursor: Default::default(),
        }
    }

    fn get_row_from_range(&mut self) -> Result<Option<Row>> {
        let range = &self.key_ranges[self.cursor];
        let kv = self.scanner.next_row(range)?;
        let (key, value) = match kv {
            Some((key, value)) => (key, value),
            None => return Ok(None),
        };
        let h = box_try!(table::decode_handle(&key));
        let row_data = box_try!(table::cut_row(value, &self.col_ids));
        let seek_key = if self.desc {
            box_try!(table::truncate_as_row_key(&key)).to_vec()
        } else {
            prefix_next(&key)
        };
        self.scanner.set_seek_key(Some(seek_key));
        Ok(Some(Row::new(h, row_data)))
    }

    fn get_row_from_point(&mut self) -> Result<Option<Row>> {
        let key = self.key_ranges[self.cursor].get_start();
        let value = self.scanner.get_row(key)?;
        if let Some(value) = value {
            let values = box_try!(table::cut_row(value, &self.col_ids));
            let h = box_try!(table::decode_handle(key));
            return Ok(Some(Row::new(h, values)));
        }
        Ok(None)
    }
}

impl<'a> Executor for TableScanExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>> {
        while self.cursor < self.key_ranges.len() {
            if is_point(&self.key_ranges[self.cursor]) {
                CORP_GET_OR_SCAN_COUNT.with_label_values(&["point"]).inc();
                let data = self.get_row_from_point()?;
                self.scanner.set_seek_key(None);
                self.cursor += 1;
                if data.is_some() {
                    return Ok(data);
                }
                continue;
            }

            let data = self.get_row_from_range()?;
            if data.is_none() {
                CORP_GET_OR_SCAN_COUNT.with_label_values(&["range"]).inc();
                self.scanner.set_seek_key(None);
                self.cursor += 1;
                continue;
            }
            return Ok(data);
        }
        Ok(None)
    }
}

#[cfg(test)]
mod test {
    use std::i64;

    use kvproto::kvrpcpb::IsolationLevel;
    use protobuf::RepeatedField;
    use tipb::schema::ColumnInfo;

    use storage::{SnapshotStore, Statistics};

    use super::*;
    use super::super::scanner::test::{get_point_range, get_range, prepare_table_data, Data,
                                      TestStore};

    const TABLE_ID: i64 = 1;
    const KEY_NUMBER: usize = 10;

    struct TableScanTestWrapper {
        data: Data,
        store: TestStore,
        table_scan: TableScan,
        ranges: Vec<KeyRange>,
        cols: Vec<ColumnInfo>,
    }

    impl TableScanTestWrapper {
        fn get_point_range(&self, handle: i64) -> KeyRange {
            get_point_range(TABLE_ID, handle)
        }
    }

    impl Default for TableScanTestWrapper {
        fn default() -> TableScanTestWrapper {
            let test_data = prepare_table_data(KEY_NUMBER, TABLE_ID);
            let test_store = TestStore::new(&test_data.kv_data);
            let mut table_scan = TableScan::new();
            // prepare cols
            let cols = test_data.get_prev_2_cols();
            let col_req = RepeatedField::from_vec(cols.clone());
            table_scan.set_columns(col_req);
            // prepare range
            let range = get_range(TABLE_ID, i64::MIN, i64::MAX);
            let key_ranges = vec![range];
            TableScanTestWrapper {
                data: test_data,
                store: test_store,
                table_scan: table_scan,
                ranges: key_ranges,
                cols: cols,
            }
        }
    }

    #[test]
    fn test_point_get() {
        let mut statistics = Statistics::default();
        let mut wrapper = TableScanTestWrapper::default();
        // point get returns none
        let r1 = wrapper.get_point_range(i64::MIN);
        // point get return something
        let handle = 0;
        let r2 = wrapper.get_point_range(handle);
        wrapper.ranges = vec![r1, r2];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner =
            TableScanExecutor::new(&wrapper.table_scan, wrapper.ranges, store, &mut statistics);

        let row = table_scanner.next().unwrap().unwrap();
        assert_eq!(row.handle, handle as i64);
        assert_eq!(row.data.len(), wrapper.cols.len());

        let expect_row = &wrapper.data.expect_rows[handle as usize];
        for col in &wrapper.cols {
            let cid = col.get_column_id();
            let v = row.data.get(cid).unwrap();
            assert_eq!(expect_row[&cid], v.to_vec());
        }
        assert!(table_scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_multiple_ranges() {
        let mut statistics = Statistics::default();
        let mut wrapper = TableScanTestWrapper::default();
        // prepare range
        let r1 = get_range(TABLE_ID, i64::MIN, 0);
        let r2 = get_range(TABLE_ID, 0, (KEY_NUMBER / 2) as i64);

        // prepare point get
        let handle = KEY_NUMBER / 2;
        let r3 = wrapper.get_point_range(handle as i64);

        let r4 = get_range(TABLE_ID, (handle + 1) as i64, i64::MAX);
        wrapper.ranges = vec![r1, r2, r3, r4];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner =
            TableScanExecutor::new(&wrapper.table_scan, wrapper.ranges, store, &mut statistics);

        for handle in 0..KEY_NUMBER {
            let row = table_scanner.next().unwrap().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(table_scanner.next().unwrap().is_none());
    }

    #[test]
    fn test_reverse_scan() {
        let mut statistics = Statistics::default();
        let mut wrapper = TableScanTestWrapper::default();
        wrapper.table_scan.set_desc(true);

        // prepare range
        let r1 = get_range(TABLE_ID, i64::MIN, 0);
        let r2 = get_range(TABLE_ID, 0, (KEY_NUMBER / 2) as i64);

        // prepare point get
        let handle = KEY_NUMBER / 2;
        let r3 = wrapper.get_point_range(handle as i64);

        let r4 = get_range(TABLE_ID, (handle + 1) as i64, i64::MAX);
        wrapper.ranges = vec![r1, r2, r3, r4];

        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let mut table_scanner =
            TableScanExecutor::new(&wrapper.table_scan, wrapper.ranges, store, &mut statistics);

        for tid in 0..KEY_NUMBER {
            let handle = KEY_NUMBER - tid - 1;
            let row = table_scanner.next().unwrap().unwrap();
            assert_eq!(row.handle, handle as i64);
            assert_eq!(row.data.len(), wrapper.cols.len());
            let expect_row = &wrapper.data.expect_rows[handle];
            for col in &wrapper.cols {
                let cid = col.get_column_id();
                let v = row.data.get(cid).unwrap();
                assert_eq!(expect_row[&cid], v.to_vec());
            }
        }
        assert!(table_scanner.next().unwrap().is_none());
    }
}
