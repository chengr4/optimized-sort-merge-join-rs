use std::fs::File;
use std::io::BufWriter;

use crate::records::Records;

mod dept;
mod employee;

use dept::DeptStrategy;
use employee::EmployeeStrategy;

// Define a strategy trait for sorting and writing records
pub trait SortWriteStrategy {
    fn sort(&self, buffers: &mut [Records], total_pages: usize);
    fn write(&self, buffers: &[Records], file: &mut BufWriter<File>, total_pages: usize);
}

pub fn get_strategy(run_name: &str) -> Box<dyn SortWriteStrategy> {
    match run_name {
        "Dept" => Box::new(DeptStrategy),
        "Employee" => Box::new(EmployeeStrategy),
        _ => panic!("Invalid run name"),
    }
}