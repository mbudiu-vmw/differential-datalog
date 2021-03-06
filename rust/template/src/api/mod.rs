mod c_api;

#[cfg(feature = "c_api")]
pub use c_api::*;

use std::ffi;
use std::fs;
use std::io;
use std::iter;
use std::mem;
use std::os::raw;

#[cfg(unix)]
use std::os::unix::io::{FromRawFd, IntoRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{FromRawHandle, IntoRawHandle, RawHandle};

use std::ptr;
use std::slice;
use std::sync::{Arc, Mutex};

use differential_datalog::ddval::*;
use differential_datalog::program::*;
use differential_datalog::record;
use differential_datalog::record::IntoRecord;
use differential_datalog::record_val_upds;
use differential_datalog::Callback;
use differential_datalog::DDlog;
use differential_datalog::DeltaMap;
use differential_datalog::RecordReplay;
use std::collections::BTreeMap;
use std::collections::BTreeSet;

use super::update_handler::*;
use super::*;

/* FlatBuffers bindings generated by `ddlog` */
#[cfg(feature = "flatbuf")]
use super::flatbuf;

#[cfg(feature = "flatbuf")]
use super::flatbuf::FromFlatBuffer;

// TODO: Move HDDlog into the differential_datalog crate.
#[derive(Debug)]
pub struct HDDlog {
    pub prog: Mutex<RunningProgram>,
    pub update_handler: Box<dyn IMTUpdateHandler>,
    pub db: Option<Arc<Mutex<DeltaMap<DDValue>>>>,
    pub deltadb: Arc<Mutex<Option<DeltaMap<DDValue>>>>,
    pub print_err: Option<extern "C" fn(msg: *const raw::c_char)>,
    /// When set, all commands sent to the program are recorded in
    /// the specified `.dat` file so that they can be replayed later.
    pub replay_file: Option<Mutex<fs::File>>,
}

// `HDDlog` is not `Send` because `WorkerGuards` are not `Send`.  Remove this
// unsafe impl once we switcht to a more recent DD.
unsafe impl Send for HDDlog {}

/* Public API */
impl HDDlog {
    pub fn print_err(f: Option<extern "C" fn(msg: *const raw::c_char)>, msg: &str) {
        match f {
            None => eprintln!("{}", msg),
            Some(f) => f(ffi::CString::new(msg).unwrap().into_raw()),
        }
    }

    pub fn eprintln(&self, msg: &str) {
        Self::print_err(self.print_err, msg)
    }

    pub fn get_table_id(tname: &str) -> Result<Relations, String> {
        Relations::try_from(tname).map_err(|()| format!("unknown relation {}", tname))
    }

    pub fn get_table_name(tid: RelId) -> Result<&'static str, String> {
        relid2name(tid).ok_or_else(|| format!("unknown relation {}", tid))
    }

    #[cfg(feature = "c_api")]
    pub fn get_table_cname(tid: RelId) -> Result<&'static ffi::CStr, String> {
        relid2cname(tid).ok_or_else(|| format!("unknown relation {}", tid))
    }

    pub fn get_index_id(iname: &str) -> Result<Indexes, String> {
        Indexes::try_from(iname).map_err(|()| format!("unknown index {}", iname))
    }

    pub fn get_index_name(iid: IdxId) -> Result<&'static str, String> {
        indexid2name(iid).ok_or_else(|| format!("unknown index {}", iid))
    }

    #[cfg(feature = "c_api")]
    pub fn get_index_cname(iid: IdxId) -> Result<&'static ffi::CStr, String> {
        indexid2cname(iid).ok_or_else(|| format!("unknown index {}", iid))
    }

    pub fn record_commands(&mut self, file: &mut Option<Mutex<fs::File>>) {
        mem::swap(&mut self.replay_file, file);
    }

    pub fn dump_input_snapshot<W>(&self, w: &mut W) -> io::Result<()>
    where
        W: io::Write,
    {
        for (rel, relname) in INPUT_RELIDMAP.iter() {
            let prog = self.prog.lock().unwrap();
            match prog.get_input_relation_data(*rel as RelId) {
                Ok(valset) => {
                    for v in valset.iter() {
                        w.record_insert(relname, v)?;
                        writeln!(w, ",")?;
                    }
                }
                _ => match prog.get_input_relation_index(*rel as RelId) {
                    Ok(ivalset) => {
                        for v in ivalset.values() {
                            w.record_insert(relname, v)?;
                            writeln!(w, ",")?;
                        }
                    }
                    _ => match prog.get_input_multiset_data(*rel as RelId) {
                        Ok(ivalmset) => {
                            for (v, weight) in ivalmset.iter() {
                                if *weight >= 0 {
                                    for _ in 0..*weight {
                                        w.record_insert(relname, v)?;
                                        writeln!(w, ",")?;
                                    }
                                } else {
                                    for _ in 0..(-*weight) {
                                        w.record_delete(relname, v)?;
                                        writeln!(w, ",")?;
                                    }
                                }
                            }
                        }
                        _ => {
                            panic!("Unknown input relation {:?} in dump_input_snapshot", rel);
                        }
                    },
                },
            }
        }
        Ok(())
    }

    pub fn clear_relation(&self, table: usize) -> Result<(), String> {
        self.record_clear_relation(table);
        self.prog.lock().unwrap().clear_relation(table)
    }

    pub fn dump_table<F>(&self, table: usize, cb: Option<F>) -> Result<(), &'static str>
    where
        F: Fn(&record::Record, isize) -> bool,
    {
        self.record_dump_table(table);
        if let Some(ref db) = self.db {
            HDDlog::db_dump_table(&mut db.lock().unwrap(), table, cb);
            Ok(())
        } else {
            Err("cannot dump table: ddlog_run() was invoked with do_store flag set to false")
        }
    }

    /// Controls recording of differential operator runtimes.  When enabled,
    /// DDlog records each activation of every operator and prints the
    /// per-operator CPU usage summary in the profile.  When disabled, the
    /// recording stops, but the previously accumulated profile is preserved.
    ///
    /// Recording CPU events can be expensive in large dataflows and is
    /// therefore disabled by default.
    pub fn enable_cpu_profiling(&self, enable: bool) {
        self.record_enable_cpu_profiling(enable);
        self.prog.lock().unwrap().enable_cpu_profiling(enable);
    }

    pub fn enable_timely_profiling(&self, enable: bool) {
        self.record_enable_timely_profiling(enable);
        self.prog.lock().unwrap().enable_timely_profiling(enable);
    }

    /// returns DDlog program runtime profile
    pub fn profile(&self) -> String {
        self.record_profile();
        let rprog = self.prog.lock().unwrap();
        let profile: String = rprog.profile.lock().unwrap().to_string();
        profile
    }
}

impl DDlog for HDDlog {
    type Convert = DDlogConverter;
    type UpdateSerializer = UpdateSerializer;

    fn run<F>(workers: usize, do_store: bool, cb: F) -> Result<(Self, DeltaMap<DDValue>), String>
    where
        Self: Sized,
        F: Callback,
    {
        Self::do_run(workers, do_store, CallbackUpdateHandler::new(cb), None)
    }

    fn transaction_start(&self) -> Result<(), String> {
        self.record_transaction_start();
        self.prog.lock().unwrap().transaction_start()
    }

    fn transaction_commit_dump_changes(&self) -> Result<DeltaMap<DDValue>, String> {
        self.record_transaction_commit(true);
        *self.deltadb.lock().unwrap() = Some(DeltaMap::new());

        self.update_handler.before_commit();
        match (self.prog.lock().unwrap().transaction_commit()) {
            Ok(()) => {
                self.update_handler.after_commit(true);
                let mut delta = self.deltadb.lock().unwrap();
                Ok(delta.take().unwrap())
            }
            Err(e) => {
                self.update_handler.after_commit(false);
                Err(e)
            }
        }
    }

    fn transaction_commit(&self) -> Result<(), String> {
        self.record_transaction_commit(false);
        self.update_handler.before_commit();

        match (self.prog.lock().unwrap().transaction_commit()) {
            Ok(()) => {
                self.update_handler.after_commit(true);
                Ok(())
            }
            Err(e) => {
                self.update_handler.after_commit(false);
                Err(e)
            }
        }
    }

    fn transaction_rollback(&self) -> Result<(), String> {
        self.record_transaction_rollback();
        self.prog.lock().unwrap().transaction_rollback()
    }

    /// Two implementations of `apply_updates`: one that takes `Record`s and one that takes `DDValue`s.
    fn apply_updates<V, I>(&self, upds: I) -> Result<(), String>
    where
        V: Deref<Target = record::UpdCmd>,
        I: iter::Iterator<Item = V>,
    {
        let mut conversion_err = false;
        let mut msg: Option<String> = None;

        // Iterate through all updates, but only feed them to `apply_valupdates` until we reach
        // the first invalid command.
        // XXX: We must iterate till the end of `upds`, as `ddlog_apply_updates` relies on this to
        // deallocate all commands.
        let res = self.apply_valupdates(upds.flat_map(|u| {
            if conversion_err {
                None
            } else {
                match updcmd2upd(u.deref()) {
                    Ok(u) => Some(u),
                    Err(e) => {
                        conversion_err = true;
                        msg = Some(format!("invalid command {:?}: {}", *u, e));
                        None
                    }
                }
            }
        }));

        match msg {
            Some(e) => Err(e),
            None => res,
        }
    }

    #[cfg(feature = "flatbuf")]
    fn apply_updates_from_flatbuf(&self, buf: &[u8]) -> Result<(), String> {
        let cmditer = flatbuf::updates_from_flatbuf(buf)?;
        let upds: Result<Vec<Update<DDValue>>, String> = cmditer
            .map(|cmd| flatbuf::DDValueUpdate::from_flatbuf(cmd).map(|x| x.0))
            .collect();
        self.apply_valupdates(upds?.into_iter())
    }

    fn apply_valupdates<I>(&self, updates: I) -> Result<(), String>
    where
        I: Iterator<Item = Update<DDValue>>,
    {
        // Make sure that the updates being inserted have the correct value types for their
        // relation
        let inspect_update: fn(&Update<DDValue>) -> Result<(), String> = |update| {
            let relation = Relations::try_from(update.relid())
                .map_err(|_| format!("unknown relation id {}", update.relid()))?;

            if let Some(value) = update.get_value() {
                if relation.type_id() != value.type_id() {
                    return Err(format!("attempted to insert the incorrect type {:?} into relation {:?} whose value type is {:?}", value.type_id(), relation, relation.type_id()));
                }
            }

            Ok(())
        };

        if let Some(ref f) = self.replay_file {
            let mut file = f.lock().unwrap();
            let updates = record_val_upds::<Self::Convert, _, _, _>(&mut *file, updates, |_| ());

            self.prog
                .lock()
                .unwrap()
                .apply_updates(updates, inspect_update)
        } else {
            self.prog
                .lock()
                .unwrap()
                .apply_updates(updates, inspect_update)
        }
    }

    fn dump_index(&self, index: IdxId) -> Result<BTreeSet<DDValue>, String> {
        self.record_dump_index(index);
        let idx = Indexes::try_from(index).map_err(|()| format!("unknown index {}", index))?;
        let arrid = indexes2arrid(idx);
        self.prog.lock().unwrap().dump_arrangement(arrid)
    }

    fn query_index(&self, index: IdxId, key: DDValue) -> Result<BTreeSet<DDValue>, String> {
        self.record_query_index(index, &key);
        let idx = Indexes::try_from(index).map_err(|()| format!("unknown index {}", index))?;
        let arrid = indexes2arrid(idx);
        self.prog.lock().unwrap().query_arrangement(arrid, key)
    }

    fn query_index_rec(
        &self,
        index: IdxId,
        key: &record::Record,
    ) -> Result<BTreeSet<DDValue>, String> {
        let idx = Indexes::try_from(index).map_err(|()| format!("unknown index {}", index))?;
        let k = idxkey_from_record(idx, key)?;
        self.record_query_index(index, &k);
        let arrid = indexes2arrid(idx);
        self.prog.lock().unwrap().query_arrangement(arrid, k)
    }

    #[cfg(feature = "flatbuf")]
    fn query_index_from_flatbuf(&self, buf: &[u8]) -> Result<BTreeSet<DDValue>, String> {
        let (idxid, key) = flatbuf::query_from_flatbuf(buf)?;
        self.query_index(idxid, key)
    }

    fn stop(&mut self) -> Result<(), String> {
        self.prog.lock().unwrap().stop()
    }
}

/* Internals */
impl HDDlog {
    fn do_run<UH>(
        workers: usize,
        do_store: bool,
        cb: UH,
        print_err: Option<extern "C" fn(msg: *const raw::c_char)>,
    ) -> Result<(Self, DeltaMap<DDValue>), String>
    where
        UH: UpdateHandler + Send + 'static,
    {
        let workers = if workers == 0 { 1 } else { workers };

        let db: Arc<Mutex<DeltaMap<DDValue>>> = Arc::new(Mutex::new(DeltaMap::new()));
        let db2 = db.clone();

        let deltadb: Arc<Mutex<Option<DeltaMap<_>>>> = Arc::new(Mutex::new(Some(DeltaMap::new())));
        let deltadb2 = deltadb.clone();

        let handler: Box<dyn IMTUpdateHandler> = {
            let handler_generator = move || {
                /* Always use delta handler, which costs nothing unless it is
                 * actually used. */
                let delta_handler = DeltaUpdateHandler::new(deltadb2);

                let store_handler = if do_store {
                    Some(ValMapUpdateHandler::new(db2))
                } else {
                    None
                };

                let cb_handler = Box::new(cb) as Box<dyn UpdateHandler + Send>;
                let mut handlers: Vec<Box<dyn UpdateHandler>> = Vec::new();
                handlers.push(Box::new(delta_handler));
                if let Some(h) = store_handler {
                    handlers.push(Box::new(h))
                };
                handlers.push(cb_handler);
                Box::new(ChainedUpdateHandler::new(handlers)) as Box<dyn UpdateHandler>
            };
            Box::new(ThreadUpdateHandler::new(handler_generator))
        };

        let program = prog(handler.mt_update_cb());

        /* Notify handler about initial transaction */
        handler.before_commit();
        let prog = program.run(workers as usize)?;
        handler.after_commit(true);

        /* Extract state after initial transaction. */
        let init_state = deltadb.lock().unwrap().take().unwrap();

        Ok((
            HDDlog {
                prog: Mutex::new(prog),
                update_handler: handler,
                db: Some(db),
                deltadb,
                print_err,
                replay_file: None,
            },
            init_state,
        ))
    }

    fn db_dump_table<F>(db: &mut DeltaMap<DDValue>, table: libc::size_t, cb: Option<F>)
    where
        F: Fn(&record::Record, isize) -> bool,
    {
        if let Some(f) = cb {
            for (val, w) in db.get_rel(table) {
                //assert!(*w == 1);
                if !f(&val.clone().into_record(), *w) {
                    break;
                }
            }
        };
    }

    fn record_transaction_start(&self) {
        if let Some(ref f) = self.replay_file {
            let _ = f.lock().unwrap().record_start().map_err(|_| {
                self.eprintln("failed to record invocation in replay file");
            });
        }
    }

    fn record_transaction_commit(&self, record_changes: bool) {
        if let Some(ref f) = self.replay_file {
            let _ = f
                .lock()
                .unwrap()
                .record_commit(record_changes)
                .map_err(|_| {
                    self.eprintln("failed to record invocation in replay file");
                });
        }
    }

    fn record_transaction_rollback(&self) {
        if let Some(ref f) = self.replay_file {
            let _ = f.lock().unwrap().record_rollback().map_err(|_| {
                self.eprintln("failed to record invocation in replay file");
            });
        }
    }

    fn record_clear_relation(&self, rid: RelId) {
        if let Some(ref f) = self.replay_file {
            let _ = f
                .lock()
                .unwrap()
                .record_clear::<DDlogConverter>(rid)
                .map_err(|e| {
                    self.eprintln("failed to record invocation in replay file");
                });
        }
    }

    fn record_dump_table(&self, rid: RelId) {
        if let Some(ref f) = self.replay_file {
            let _ = f
                .lock()
                .unwrap()
                .record_dump::<DDlogConverter>(rid)
                .map_err(|e| {
                    self.eprintln("ddlog_dump_table(): failed to record invocation in replay file");
                });
        }
    }

    fn record_dump_index(&self, iid: IdxId) {
        if let Some(ref f) = self.replay_file {
            let _ = f
                .lock()
                .unwrap()
                .record_dump_index::<DDlogConverter>(iid)
                .map_err(|e| {
                    self.eprintln("ddlog_dump_index(): failed to record invocation in replay file");
                });
        }
    }

    fn record_query_index(&self, iid: IdxId, key: &DDValue) {
        if let Some(ref f) = self.replay_file {
            let _ = f
                .lock()
                .unwrap()
                .record_query_index::<DDlogConverter>(iid, key)
                .map_err(|e| {
                    self.eprintln("ddlog_dump_index(): failed to record invocation in replay file");
                });
        }
    }

    fn record_enable_cpu_profiling(&self, enable: bool) {
        if let Some(ref f) = self.replay_file {
            let _ = f.lock().unwrap().record_cpu_profiling(enable).map_err(|_| {
                self.eprintln(
                    "ddlog_cpu_profiling_enable(): failed to record invocation in replay file",
                )
            });
        }
    }

    fn record_enable_timely_profiling(&self, enable: bool) {
        if let Some(ref f) = self.replay_file {
            let _ = f
                .lock()
                .unwrap()
                .record_timely_profiling(enable)
                .map_err(|_| {
                    self.eprintln(
                    "ddlog_timely_profiling_enable(): failed to record invocation in replay file",
                )
                });
        }
    }

    fn record_profile(&self) {
        if let Some(ref f) = self.replay_file {
            let _ = f.lock().unwrap().record_profile().map_err(|_| {
                self.eprintln("record_profile: failed to record invocation in replay file");
            });
        }
    }
}

pub fn updcmd2upd(c: &record::UpdCmd) -> Result<Update<DDValue>, String> {
    match c {
        record::UpdCmd::Insert(rident, rec) => {
            let relid =
                Relations::try_from(rident).map_err(|_| format!("Unknown relation {}", rident))?;
            let val = relval_from_record(relid, rec)?;
            Ok(Update::Insert {
                relid: relid as RelId,
                v: val,
            })
        }
        record::UpdCmd::InsertOrUpdate(rident, rec) => {
            let relid =
                Relations::try_from(rident).map_err(|_| format!("Unknown relation {}", rident))?;
            let val = relval_from_record(relid, rec)?;
            Ok(Update::InsertOrUpdate {
                relid: relid as RelId,
                v: val,
            })
        }
        record::UpdCmd::Delete(rident, rec) => {
            let relid =
                Relations::try_from(rident).map_err(|()| format!("Unknown relation {}", rident))?;
            let val = relval_from_record(relid, rec)?;
            Ok(Update::DeleteValue {
                relid: relid as RelId,
                v: val,
            })
        }
        record::UpdCmd::DeleteKey(rident, rec) => {
            let relid =
                Relations::try_from(rident).map_err(|()| format!("Unknown relation {}", rident))?;
            let key = relkey_from_record(relid, rec)?;
            Ok(Update::DeleteKey {
                relid: relid as RelId,
                k: key,
            })
        }
        record::UpdCmd::Modify(rident, key, rec) => {
            let relid =
                Relations::try_from(rident).map_err(|()| format!("Unknown relation {}", rident))?;
            let key = relkey_from_record(relid, key)?;
            Ok(Update::Modify {
                relid: relid as RelId,
                k: key,
                m: Arc::new(rec.clone()),
            })
        }
    }
}
