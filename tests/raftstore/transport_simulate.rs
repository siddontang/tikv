use kvproto::raft_serverpb::RaftMessage;
use tikv::raftstore::Result;
use tikv::raftstore::store::Transport;
use rand;
use std::sync::{Arc, RwLock};

use super::util::*;
use self::Strategy::*;

#[derive(Clone)]
pub enum Strategy {
    LossPacket(u32),
    Latency(u64),
    OutOrder,
}

trait Filter: Send + Sync {
    // in a SimulateTransport, if any filter's before return true, msg will be discard
    fn before(&self, msg: &RaftMessage) -> bool;
    // with after provided, one can change the return value arbitrarily
    fn after(&self, Result<()>) -> Result<()>;
}

struct FilterLossPacket(u32);
struct FilterLatency(u64);
struct FilterOutOrder;

impl Filter for FilterLossPacket {
    fn before(&self, _: &RaftMessage) -> bool {
        rand::random::<u32>() % 100u32 < self.0
    }
    fn after(&self, x: Result<()>) -> Result<()> {
        x
    }
}

impl Filter for FilterLatency {
    fn before(&self, _: &RaftMessage) -> bool {
        sleep_ms(self.0);
        false
    }
    fn after(&self, x: Result<()>) -> Result<()> {
        x
    }
}

impl Filter for FilterOutOrder {
    fn before(&self, _: &RaftMessage) -> bool {
        unimplemented!()
    }
    fn after(&self, _: Result<()>) -> Result<()> {
        unimplemented!()
    }
}

pub struct SimulateTransport<T: Transport> {
    filters: Vec<Box<Filter>>,
    trans: Arc<RwLock<T>>,
}

impl<T: Transport> SimulateTransport<T> {
    pub fn new(strategy: Vec<Strategy>, trans: Arc<RwLock<T>>) -> SimulateTransport<T> {
        let mut filters: Vec<Box<Filter>> = vec![];
        for s in strategy {
            match s {
                LossPacket(rate) => {
                    filters.push(Box::new(FilterLossPacket(rate)));
                }
                Latency(latency) => {
                    filters.push(Box::new(FilterLatency(latency)));
                }
                OutOrder => {
                    filters.push(Box::new(FilterOutOrder));
                }
            }
        }

        SimulateTransport {
            filters: filters,
            trans: trans,
        }
    }
}

impl<T: Transport> Transport for SimulateTransport<T> {
    fn send(&self, msg: RaftMessage) -> Result<()> {
        let mut discard = false;
        for strategy in &self.filters {
            if strategy.before(&msg) {
                discard = true;
            }
        }

        let mut res = Ok(());
        if !discard {
            res = self.trans.read().unwrap().send(msg);
        }

        for strategy in self.filters.iter().rev() {
            res = strategy.after(res);
        }

        res
    }
}
