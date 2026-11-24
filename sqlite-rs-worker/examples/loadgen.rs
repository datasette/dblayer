//! A load generator that is not Python, so the parent's event loop cannot
//! hide a difference between workers: it drives a multi-tenant worker
//! (`--multi`, the C worker's or this crate's) over its pipes with one
//! request in flight per connection, each connection on its own database,
//! and reports requests per second and the CPU time of the worker and of
//! itself per request.
//!
//!   cargo run --release --example loadgen -- BINARY CLIENTS SECONDS DIR [codec]
//!
//! The request is a one-row read by primary key (`select id, title from r
//! where id = ?`), JSON or MessagePack. Replies are matched by id from the
//! frame bytes, not decoded.

use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::time::Instant;

fn frame(payload: &[u8]) -> Vec<u8> {
    let mut f = (payload.len() as u32).to_be_bytes().to_vec();
    f.extend_from_slice(payload);
    f
}

fn read_frame(r: &mut impl Read) -> Vec<u8> {
    let mut h = [0u8; 4];
    r.read_exact(&mut h).expect("worker closed");
    let mut b = vec![0u8; u32::from_be_bytes(h) as usize];
    r.read_exact(&mut b).expect("worker closed");
    b
}

/// The id of a reply: JSON `"id": N` or MessagePack `a2 'id' <int>`.
fn reply_id(b: &[u8], msgpack: bool) -> i64 {
    if msgpack {
        let i = b.windows(3).position(|w| w == [0xa2, b'i', b'd']).expect("id") + 3;
        return match b[i] {
            x @ 0..=0x7f => x as i64,
            0xcc => b[i + 1] as i64,
            0xcd => u16::from_be_bytes([b[i + 1], b[i + 2]]) as i64,
            0xce => u32::from_be_bytes(b[i + 1..i + 5].try_into().unwrap()) as i64,
            0xcf => u64::from_be_bytes(b[i + 1..i + 9].try_into().unwrap()) as i64,
            x => panic!("id byte {x:#x}"),
        };
    }
    let s = std::str::from_utf8(b).unwrap();
    let i = s.find("\"id\":").expect("id") + 5;
    let t: String = s[i..].trim_start().chars().take_while(|c| c.is_ascii_digit()).collect();
    t.parse().unwrap()
}

fn mp_str(o: &mut Vec<u8>, s: &str) {
    assert!(s.len() < 256);
    if s.len() < 32 {
        o.push(0xa0 | s.len() as u8);
    } else {
        o.push(0xd9);
        o.push(s.len() as u8);
    }
    o.extend_from_slice(s.as_bytes());
}

fn mp_int(o: &mut Vec<u8>, v: i64) {
    o.push(0xd3);
    o.extend_from_slice(&v.to_be_bytes());
}

fn request(msgpack: bool, sql: &str, id: i64, conn: i64) -> Vec<u8> {
    if !msgpack {
        return frame(format!(r#"{{"type":"execute","sql":"{sql}","params":[7],"id":{id},"conn":{conn}}}"#).as_bytes());
    }
    let mut o = vec![0x85];
    mp_str(&mut o, "type");
    mp_str(&mut o, "execute");
    mp_str(&mut o, "sql");
    mp_str(&mut o, sql);
    mp_str(&mut o, "params");
    o.push(0x91);
    o.push(7);
    mp_str(&mut o, "id");
    mp_int(&mut o, id);
    mp_str(&mut o, "conn");
    mp_int(&mut o, conn);
    frame(&o)
}

fn cpu_s(pid: u32) -> f64 {
    // Linux: /proc (clock ticks; USER_HZ is 100 on every mainstream Linux
    // build). procps `ps -o time` there has whole seconds only.
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        && let Some((_, rest)) = stat.rsplit_once(')')
    {
        let f: Vec<&str> = rest.split_whitespace().collect();
        if f.len() > 12 {
            let ticks = f[11].parse::<f64>().unwrap_or(0.0) + f[12].parse::<f64>().unwrap_or(0.0);
            return ticks / 100.0;
        }
    }
    let out = Command::new("ps").args(["-o", "time=", "-p", &pid.to_string()]).output().unwrap();
    let t = String::from_utf8_lossy(&out.stdout).trim().replace('-', ":");
    t.split(':').fold(0.0, |a, p| a * 60.0 + p.parse::<f64>().unwrap_or(0.0))
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    if a.len() < 5 {
        eprintln!("usage: loadgen BINARY CLIENTS SECONDS DIR [json|msgpack]");
        std::process::exit(2);
    }
    let (bin, clients, secs, dir) = (&a[1], a[2].parse::<i64>().unwrap(), a[3].parse::<f64>().unwrap(), &a[4]);
    let msgpack = a.get(5).map(|s| s == "msgpack").unwrap_or(false);
    let mut child = Command::new(bin).arg("--multi").stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
    let pid = child.id();
    let mut w = child.stdin.take().unwrap();
    let mut r = std::io::BufReader::with_capacity(1 << 16, child.stdout.take().unwrap());
    let codecs = if msgpack { "\"msgpack\",\"json\"" } else { "\"json\"" };
    w.write_all(&frame(format!(r#"{{"type":"hello","protocol":1,"codecs":[{codecs}],"id":1}}"#).as_bytes())).unwrap();
    read_frame(&mut r); // hello is always JSON
    // Open and seed every database (JSON setup frames would not decode
    // under msgpack, so setup uses the same codec via a tiny encoder).
    let mut id = 10;
    let call = |w: &mut std::process::ChildStdin, r: &mut std::io::BufReader<_>, payload: Vec<u8>| {
        w.write_all(&payload).unwrap();
        read_frame(r)
    };
    let mut conns = Vec::new();
    for i in 0..clients {
        id += 1;
        let path = format!("{dir}/lg-{i}.db");
        let p = if msgpack {
            let mut o = vec![0x84];
            mp_str(&mut o, "type");
            mp_str(&mut o, "open");
            mp_str(&mut o, "path");
            mp_str(&mut o, &path);
            mp_str(&mut o, "mode");
            mp_str(&mut o, "write");
            mp_str(&mut o, "id");
            mp_int(&mut o, id);
            frame(&o)
        } else {
            frame(format!(r#"{{"type":"open","path":"{path}","mode":"write","id":{id}}}"#).as_bytes())
        };
        let rep = call(&mut w, &mut r, p);
        // conn ids are assigned 1, 2, 3, ... in open order
        assert!(!String::from_utf8_lossy(&rep).contains("error"), "open failed");
        conns.push(i + 1);
    }
    let setup = "create table if not exists r (id integer primary key, title text)";
    let fill = "insert or ignore into r (id, title) values (7, 'title 7')";
    for &c in &conns {
        for sql in [setup, fill] {
            id += 1;
            let req = if !msgpack {
                frame(format!(r#"{{"type":"execute","sql":"{sql}","id":{id},"conn":{c}}}"#).as_bytes())
            } else {
                // same message without params
                let mut o = vec![0x84];
                mp_str(&mut o, "type");
                mp_str(&mut o, "execute");
                mp_str(&mut o, "sql");
                mp_str(&mut o, sql);
                mp_str(&mut o, "id");
                mp_int(&mut o, id);
                mp_str(&mut o, "conn");
                mp_int(&mut o, c);
                frame(&o)
            };
            call(&mut w, &mut r, req);
        }
    }
    let sql = "select id, title from r where id = ?";
    let mut owner = std::collections::HashMap::new();
    let send = |w: &mut std::process::ChildStdin, id: i64, c: i64| w.write_all(&request(msgpack, sql, id, c)).unwrap();
    let (cpu0, me0) = (cpu_s(pid), cpu_s(std::process::id()));
    let t0 = Instant::now();
    for &c in &conns {
        id += 1;
        owner.insert(id, c);
        send(&mut w, id, c);
    }
    w.flush().unwrap();
    let mut done: u64 = 0;
    loop {
        let rep = read_frame(&mut r);
        let rid = reply_id(&rep, msgpack);
        let c = owner.remove(&rid).expect("unknown reply id");
        done += 1;
        if t0.elapsed().as_secs_f64() < secs {
            id += 1;
            owner.insert(id, c);
            send(&mut w, id, c);
        } else if owner.is_empty() {
            break;
        }
    }
    let wall = t0.elapsed().as_secs_f64();
    let (cpu1, me1) = (cpu_s(pid), cpu_s(std::process::id()));
    drop(w);
    let _ = child.wait();
    let rps = done as f64 / wall;
    println!(
        "{{\"clients\": {clients}, \"codec\": \"{}\", \"requests\": {done}, \"rps\": {rps:.0}, \"worker_cpu_cores\": {:.3}, \
\"worker_cpu_us_per_request\": {:.2}, \"driver_cpu_cores\": {:.3}}}",
        if msgpack { "msgpack" } else { "json" },
        (cpu1 - cpu0) / wall,
        (cpu1 - cpu0) / done as f64 * 1e6,
        (me1 - me0) / wall
    );
}
