use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::io::{self, BufRead, BufReader, Write};
use std::{fs::File, io::BufWriter, sync::Mutex};

use optimized_sort_merge_join_rs::records::{
    DeptRecord, EmpRecord, Records, grab_dept_record, grab_emp_record,
};
use optimized_sort_merge_join_rs::strategy::get_strategy;

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
    let strategy = get_strategy(run_name);

    // Sort the buffer
    {
        let mut buffers = BUFFERS.lock().unwrap();
        strategy.sort(&mut buffers[..], total_pages_to_sort);
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

    // Write to file
    {
        let buffers = BUFFERS.lock().unwrap();
        strategy.write(&buffers[..], &mut file, total_pages_to_sort);
    }
}

fn print_join(
    dept_run_number: usize,
    emp_run_number: usize,
    join_out: &mut File,
) -> io::Result<()> {
    let buffer = BUFFERS.lock().unwrap();
    let dept = &buffer[dept_run_number];
    let emp = &buffer[emp_run_number];

    writeln!(
        join_out,
        "{},{},{},{},{},{},{}",
        dept.dept_record.did,
        dept.dept_record.dname,
        dept.dept_record.manager_id,
        emp.emp_record.id,
        emp.emp_record.name,
        emp.emp_record.bio,
        emp.emp_record.manager_id
    )
}

fn merge_join_runs(
    dept_run_numbers: usize,
    emp_run_numbers: usize,
    join_out: &mut File,
) -> io::Result<()> {
    if emp_run_numbers + dept_run_numbers > BUFFER_SIZE - 1 {
        panic!("Not enough space in buffer to merge join runs");
    }

    // Open temporary run files for Dept
    let mut dept_readers: Vec<Option<BufReader<File>>> = Vec::with_capacity(dept_run_numbers);
    // Priority queue stores (manager_id, buffer_index) in increasing order.
    let mut dept_pq: BinaryHeap<Reverse<(usize, usize)>> = BinaryHeap::new();

    // Read the first record from each Dept run file
    for i in 0..dept_run_numbers {
        let dept_run_file_name = format!("run_Dept_{}.tmp", i);
        let dept_file = File::open(&dept_run_file_name)
            .expect(&format!("Error opening {}", dept_run_file_name));
        let mut dept_reader = BufReader::new(dept_file);
        let mut line = String::new();
        if dept_reader.read_line(&mut line)? > 0 {
            let parts: Vec<&str> = line.trim_end().split(',').collect();
            let did = parts[0].parse::<usize>().unwrap_or_default();
            let dname = parts[1].to_string();
            let manager_id = parts[2].parse::<usize>().unwrap_or_default();
            let record = Records {
                dept_record: DeptRecord {
                    did,
                    dname,
                    manager_id,
                },
                ..Default::default()
            };
            {
                let mut buffers = BUFFERS.lock().unwrap();
                buffers[i] = record;
            }
            dept_pq.push(Reverse((manager_id, i)));
        }
        dept_readers.push(Some(dept_reader));
    }

    // Open the temporary run files for Employee.
    let mut emp_readers: Vec<Option<BufReader<File>>> = Vec::with_capacity(emp_run_numbers);
    // Priority queue stores (manager_id, buffer_index) in increasing order.
    let mut emp_pq: BinaryHeap<Reverse<(usize, usize)>> = BinaryHeap::new();
    // Read the first record from each Employee run file
    for i in 0..emp_run_numbers {
        let emp_run_file_name = format!("run_Employee_{}.tmp", i);
        let emp_file =
            File::open(&emp_run_file_name).expect(&format!("Error opening {}", emp_run_file_name));
        let mut emp_reader = BufReader::new(emp_file);
        let mut line = String::new();
        if emp_reader.read_line(&mut line)? > 0 {
            let parts: Vec<&str> = line.trim_end().split(',').collect();
            let id = parts[0].parse::<usize>().unwrap_or_default();
            let name = parts[1].to_string();
            let bio = parts[2].to_string();
            let manager_id = parts[3].parse::<usize>().unwrap_or_default();
            let record = Records {
                emp_record: EmpRecord {
                    id,
                    name,
                    bio,
                    manager_id,
                },
                ..Default::default()
            };

            let mut buffers = BUFFERS.lock().unwrap();
            buffers[i + dept_run_numbers] = record;
            emp_pq.push(Reverse((id, i + dept_run_numbers)));
        }
        emp_readers.push(Some(emp_reader));
    }

    // Merge join the runs
    while !dept_pq.is_empty() && !emp_pq.is_empty() {
        let Reverse((dept_key, dept_buffer_index)) = *dept_pq.peek().unwrap();
        let Reverse((emp_key, emp_buffer_index)) = *emp_pq.peek().unwrap();

        if dept_key == emp_key {
            let _ = print_join(dept_buffer_index, emp_buffer_index, join_out);
            dept_pq.pop();
            let dept_reader = dept_readers[dept_buffer_index].as_mut().unwrap();
            let mut line = String::new();
            if dept_reader.read_line(&mut line)? > 0 {
                let parts: Vec<&str> = line.trim_end().split(',').collect();
                let did = parts[0].parse::<usize>().unwrap_or_default();
                let dname = parts[1].to_string();
                let manager_id = parts[2].parse::<usize>().unwrap_or_default();
                let record = Records {
                    dept_record: DeptRecord {
                        did,
                        dname,
                        manager_id,
                    },
                    ..Default::default()
                };
                let mut buffers = BUFFERS.lock().unwrap();
                buffers[dept_buffer_index] = record;
                dept_pq.push(Reverse((manager_id, dept_buffer_index)));
            }
        } else if dept_key < emp_key {
            dept_pq.pop();
            let dept_reader = dept_readers[dept_buffer_index].as_mut().unwrap();
            let mut line = String::new();
            if dept_reader.read_line(&mut line)? > 0 {
                let parts: Vec<&str> = line.trim_end().split(',').collect();
                let did = parts[0].parse::<usize>().unwrap_or_default();
                let dname = parts[1].to_string();
                let manager_id = parts[2].parse::<usize>().unwrap_or_default();
                let record = Records {
                    dept_record: DeptRecord {
                        did,
                        dname,
                        manager_id,
                    },
                    ..Default::default()
                };
                let mut buffers = BUFFERS.lock().unwrap();
                buffers[dept_buffer_index] = record;
                dept_pq.push(Reverse((manager_id, dept_buffer_index)));
            }
        } else {
            emp_pq.pop();
            let emp_reader = emp_readers[emp_buffer_index - dept_run_numbers]
                .as_mut()
                .unwrap();
            let mut line = String::new();
            if emp_reader.read_line(&mut line)? > 0 {
                let parts: Vec<&str> = line.trim_end().split(',').collect();
                let id = parts[0].parse::<usize>().unwrap_or_default();
                let name = parts[1].to_string();
                let bio = parts[2].to_string();
                let manager_id = parts[3].parse::<usize>().unwrap_or_default();
                let record = Records {
                    emp_record: EmpRecord {
                        id,
                        name,
                        bio,
                        manager_id,
                    },
                    ..Default::default()
                };
                let mut buffers = BUFFERS.lock().unwrap();
                buffers[emp_buffer_index] = record;
                emp_pq.push(Reverse((id, emp_buffer_index)));
            }
        }
    }

    Ok(())
}

fn main() {
    // Open source CSV files
    let dept_in = File::open("Dept_p2.csv").expect("Cannot open Dept_p2.csv");
    let emp_in: File = File::open("Employee_p2.csv").expect("Cannot open Employee_p2.csv");
    let mut join_out: File = File::create("Join.csv").expect("Error creating Join.csv");

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
            sort_buffer("Dept", number_of_dept_runs, records_in_current_dept_run);
            records_in_current_dept_run = 0;
            number_of_dept_runs += 1;
        }

        record = grab_dept_record(&mut dept_reader);
    }

    if records_in_current_dept_run > 0 {
        sort_buffer("Dept", number_of_dept_runs, records_in_current_dept_run);
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
            sort_buffer("Employee", number_of_emp_runs, records_in_current_emp_run);
            records_in_current_emp_run = 0;
            number_of_emp_runs += 1;
        }

        record = grab_emp_record(&mut emp_reader);
    }

    if records_in_current_emp_run > 0 {
        sort_buffer("Employee", number_of_emp_runs, records_in_current_emp_run);
        number_of_emp_runs += 1;
    }

    // Use merge_join_runs() to join the runs of Dept and Employee relations and generate Join.csv
    let _ = merge_join_runs(number_of_dept_runs, number_of_emp_runs, &mut join_out);
    print!("Join completed successfully.\n");

    // Delete the temporary files (runs) after you've joined both Employee_p2.csv and Dept_p2.csv
    for i in 0..number_of_dept_runs {
        let dept_run_file_name = format!("run_Dept_{}.tmp", i);
        let _ = std::fs::remove_file(&dept_run_file_name);
    }

    for i in 0..number_of_emp_runs {
        let emp_run_file_name = format!("run_Employee_{}.tmp", i);
        let _ = std::fs::remove_file(&emp_run_file_name);
    }
}
