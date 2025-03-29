use std::io::BufWriter;
use std::fs::File;
use std::io::Write;

use super::SortWriteStrategy;
use crate::records::Records;

pub(crate) struct DeptStrategy;
impl SortWriteStrategy for DeptStrategy {
    fn sort(&self, buffers: &mut [Records], total_pages: usize) {
        buffers[..total_pages].sort_by(|a, b| a.dept_record.manager_id.cmp(&b.dept_record.manager_id));
    }
    
    fn write(&self, buffers: &[Records], file: &mut BufWriter<File>, total_pages: usize) {
        for i in 0..total_pages {
            let d = &buffers[i].dept_record;
            writeln!(file, "{},{},{}", d.did, d.dname, d.manager_id).ok();
        }
    }
}