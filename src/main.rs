use std::{fs::File, io::BufWriter, sync::Mutex};
use std::io::Write;

use optimized_sort_merge_join_rs::records::{grab_dept_record, grab_emp_record, Records};

const BUFFER_SIZE: usize = 500;

use std::sync::LazyLock;

static BUFFERS: LazyLock<Mutex<[Records; BUFFER_SIZE]>> =
    LazyLock::new(|| Mutex::new(core::array::from_fn(|_| Records::default())));

// TODO:
// 1. create global sort map and write map
// 2. create separate sort and write functions for emp and dept
// 3. sort buffer is strategy pattern

// Register strategy for sorting and writing

fn sort_buffer(run_name: &str, current_run_number: usize, total_pages_to_sort: usize) {
    if run_name == "Dept" {
        let mut buffers = BUFFERS.lock().unwrap();
        buffers[..total_pages_to_sort]
            .sort_by(|a, b| a.dept_record.manager_id.cmp(&b.dept_record.manager_id));
    } else if run_name == "Employee" {
        let mut buffers = BUFFERS.lock().unwrap();
        buffers[..total_pages_to_sort].sort_by(|a, b| a.emp_record.id.cmp(&b.emp_record.id));
    } else {
        panic!("Invalid run name");
    }

    // Write the sorted buffer into a temporary run file
    let filename = format!("run_{}_{}.tmp", run_name, current_run_number);
    let mut file = match File::create(&filename) {
        Ok(f) => BufWriter::new(f),
        Err(_) => {
            eprintln!("Error creating file {}", filename);
            return;
        }
    };

    if run_name == "Dept" {
        for i in 0..total_pages_to_sort {
            let buffers = BUFFERS.lock().unwrap();
            let d = &buffers[i].dept_record;
            writeln!(file, "{},{},{}", d.did, d.dname, d.manager_id).ok();
        }
    } else if run_name == "Employee" {
        for i in 0..total_pages_to_sort {
            let buffers = BUFFERS.lock().unwrap();

            let e = &buffers[i].emp_record;
            writeln!(file, "{},{},{},{}", e.id, e.name, e.bio, e.manager_id).ok();
        }
    } else {
        panic!("Invalid run name");
    }
}

fn print_join() {
    // Printing logic here
}

fn merge_join_runs() {}

fn main() {
    // Open source CSV files
    let dept_in = File::open("Dept_p2.csv").expect("Cannot open Dept_p2.csv");
    let emp_in: File = File::open("Employee_p2.csv").expect("Cannot open Employee_p2.csv");
    let join_out: File = File::create("Join.csv").expect("Error creating Join.csv");

    // Create sorted runs for Dept and Employee using sort_buffer()
    let mut records_in_current_dept_run = 0;
    let mut number_of_dept_runs = 0;
    let mut dept_reader = std::io::BufReader::new(dept_in);
    let mut record = grab_dept_record(&mut dept_reader);
    while record.no_values != -1 {
        {
            let mut buffers = BUFFERS.lock().unwrap();
            buffers[records_in_current_dept_run] = record;
        }

        records_in_current_dept_run += 1;
        if records_in_current_dept_run == BUFFER_SIZE {
            sort_buffer(
                "Dept",
                number_of_dept_runs,
                records_in_current_dept_run,
            );
            records_in_current_dept_run = 0;
            number_of_dept_runs += 1;
        }

        record = grab_dept_record(&mut dept_reader);
    }

    if records_in_current_dept_run > 0 {
        sort_buffer(
            "Dept",
            number_of_dept_runs,
            records_in_current_dept_run,
        );
        number_of_dept_runs += 1;
    }

    let mut records_in_current_emp_run = 0;
    let mut number_of_emp_runs = 0;
    let mut emp_reader = std::io::BufReader::new(emp_in);
    let mut record = grab_emp_record(&mut emp_reader);
    while record.no_values != -1 {
        {
            let mut buffers = BUFFERS.lock().unwrap();
            buffers[records_in_current_emp_run] = record;
        }

        records_in_current_emp_run += 1;
        if records_in_current_emp_run == BUFFER_SIZE {
            sort_buffer(
                "Employee",
                number_of_emp_runs,
                records_in_current_emp_run,
            );
            records_in_current_emp_run = 0;
            number_of_emp_runs += 1;
        }

        record = grab_emp_record(&mut emp_reader);
    }

    if records_in_current_emp_run > 0 {
        sort_buffer(
            "Employee",
            number_of_emp_runs,
            records_in_current_emp_run,
        );
        number_of_emp_runs += 1;
    }

    // Use merge_join_runs() to join the runs of Dept and Employee relations and generate Join.csv

    // Delete the temporary files (runs) after you've joined both
    // for i in 0..number_of_dept_runs {
    //     let dept_run_file_name = format!("run_Dept_{}.tmp", i);
    //     let _ = std::fs::remove_file(&dept_run_file_name);
    // }

    // for i in 0..emp_run_number {
    //     let emp_run_file_name = format!("run_Employee_{}.tmp", i);
    //     let _ = std::fs::remove_file(&emp_run_file_name);
    // }

    // Employee_p2.csv and Dept_p2.csv
}
