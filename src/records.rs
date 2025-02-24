use std::io::BufRead;

#[derive(Debug, Default)]
pub struct EmpRecord {
    id: i32,
    name: String,
    bio: String,
    manager_id: i32, 
}

#[derive(Debug, Default)]
pub struct DeptRecord {
    did: i32,
    dname: String,
    manager_id: i32, 
}

#[derive(Debug, Default)]
pub struct Records {
    pub emp_record: EmpRecord,
    pub dept_record: DeptRecord,
    pub no_values: i32,
}

pub fn grab_emp_record<T: BufRead>( empin: &mut T) -> Records {
    let mut line = String::new();
    if let Ok(bytes) = empin.read_line(&mut line) {
        if bytes > 0 {
            let parts: Vec<&str> = line.trim_end().split(',').collect();
            if parts.len() == 4 {
                let id = parts[0].parse::<i32>().unwrap_or_default();
                let name = parts[1].to_string();
                let bio = parts[2].to_string();
                let manager_id = parts[3].parse::<i32>().unwrap_or_default();
                return Records {
                    emp_record: EmpRecord {
                        id,
                        name,
                        bio,
                        manager_id,
                    },
                    ..Default::default()
                };
            }
        }
    }
    let mut rec = Records::default();
        rec.no_values = -1;
        rec

}