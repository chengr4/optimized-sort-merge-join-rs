use super::SortWriteStrategy;
use std::io::BufWriter;
use std::fs::File;
use std::io::Write;
use crate::records::Records;

pub(crate) struct EmployeeStrategy;
impl SortWriteStrategy for EmployeeStrategy {
    fn sort(&self, buffers: &mut [Records], total_pages: usize) {
        buffers[..total_pages].sort_by(|a, b| a.emp_record.id.cmp(&b.emp_record.id));
    }
    
    fn write(&self, buffers: &[Records], file: &mut BufWriter<File>, total_pages: usize) {
        for i in 0..total_pages {
            let e = &buffers[i].emp_record;
            writeln!(file, "{},{},{},{}", e.id, e.name, e.bio, e.manager_id).ok();
        }
    }
}