extern crate tikv;
extern crate fs2;
extern crate toml;
extern crate rocksdb;

use std::env;
use std::thread;
use std::process;
use std::io::Read;
use std::fs::File;
use std::path::Path;
use std::time::Duration;

use fs2::FileExt;
use rocksdb::DB;

use tikv::config::TiKvConfig;
use tikv::storage::ALL_CFS;
use tikv::util::rocksdb::new_engine_opt;
use tikv::util::rocksdb::engine_metrics::ROCKSDB_PENDING_COMPACTION_BYTES;

fn main() {
    let args: Vec<_> = env::args().collect();
    if args.len() != 2 {
        println!("usage: {} config.toml", args[0]);
        process::exit(1);
    }

    let cfg = load_config(&args[1]);
    let db = open_rocksdb(&cfg);

    for cfname in ALL_CFS {
        println!("Compacting column family {} ...", cfname);
        let cf = db.cf_handle(cfname).unwrap();
        db.compact_range_cf(cf, None, None);
        loop {
            if let Some(bytes) = db.get_property_int_cf(cf, ROCKSDB_PENDING_COMPACTION_BYTES) {
                println!("[{}] pending compaction bytes {}", cfname, bytes);
                if bytes == 0 {
                    break;
                }
            }
            thread::sleep(Duration::from_secs(10));
        }
    }
}

fn load_config(fname: &str) -> TiKvConfig {
    let mut s = String::new();
    File::open(fname).unwrap().read_to_string(&mut s).unwrap();
    toml::from_str(&s).unwrap()
}

fn open_rocksdb(cfg: &TiKvConfig) -> DB {
    let store_path = Path::new(&cfg.storage.data_dir);
    let lock_path = store_path.join(Path::new("LOCK"));
    let db_path = store_path.join(Path::new("db"));

    let f = File::create(lock_path).unwrap();
    if f.try_lock_exclusive().is_err() {
        println!("lock {:?} failed, maybe another instance is using this directory.", store_path);
        process::exit(1);
    }

    let opts = cfg.rocksdb.build_opt();
    let cfs_opts = cfg.rocksdb.build_cf_opts();
    new_engine_opt(db_path.to_str().unwrap(), opts, cfs_opts).unwrap()
}
