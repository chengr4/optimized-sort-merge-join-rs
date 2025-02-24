use std::{fs::File, sync::OnceLock};

use optimized_sort_merge_join_rs::records::Records;

const BUFFER_SIZE: usize = 500;

static BUFFERS: OnceLock<[Records; BUFFER_SIZE]> = OnceLock::new();

fn sort_buffer(run_name: String, current_run_number: i32, total_pages_to_sort: i32) {
    // Sorting logic here

}

fn print_join() {
    // Printing logic here

}

fn merge_join_runs() {}

fn main() {
    // init buffers
    let buffers = BUFFERS.get().expect("Failed to initialize buffers");

    // Open source CSV files
    let dept_in = File::open("Dept_p2.csv").expect("Cannot open Dept_p2.csv");
    let emp_in: File = File::open("Employee_p2.csv").expect("Cannot open Employee_p2.csv");
    let join_out: File = File::create("Join.csv").expect("Error creating Join.csv");

    // Create sorted runs for Dept and Employee using sort_buffer()

    // Use merge_join_runs() to join the runs of Dept and Employee relations and generate Join.csv

    // Delete the temporary files (runs) after you've joined both
    // Employee_p2.csv and Dept_p2.csv
}
