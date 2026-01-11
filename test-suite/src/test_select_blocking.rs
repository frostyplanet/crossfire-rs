use crossfire::select::Select;
use crossfire::{mpmc, mpsc, spsc};
use std::thread;
use std::time::Duration;

#[test]
fn test_select_basic() {
    let (tx1, rx1) = mpmc::bounded_blocking::<i32>(10);
    let (tx2, rx2) = mpsc::bounded_blocking::<i32>(10);

    tx1.send(100).unwrap();
    tx2.send(200).unwrap();

    let mut select = Select::new();
    select.add(&rx1);
    select.add(&rx2);

    let mut results = Vec::new();

    // Select twice
    for _ in 0..2 {
        let res = select.select().unwrap();
        if res == rx1 {
            results.push(rx1.read_select(res).unwrap());
        } else if res == rx2 {
            results.push(rx2.read_select(res).unwrap());
        } else {
            panic!("Unexpected token");
        }
    }

    results.sort();
    assert_eq!(results, vec![100, 200]);
}

#[test]
fn test_select_timeout() {
    let (_tx1, rx1) = mpmc::bounded_blocking::<i32>(10);
    let mut select = Select::new();
    select.add(&rx1);

    let start = std::time::Instant::now();
    let res = select.select_timeout(Duration::from_millis(100));
    let elapsed = start.elapsed();

    assert!(res.is_err());
    assert!(elapsed >= Duration::from_millis(100));
}

#[test]
fn test_select_disconnect() {
    let (tx1, rx1) = mpmc::bounded_blocking::<i32>(10);
    let mut select = Select::new();
    select.add(&rx1);

    drop(tx1);

    let res = select.select();
    assert!(res.is_ok());
    let res = res.unwrap();
    assert!(res == rx1);
    // Disconnected and empty
    assert!(rx1.read_select(res).is_err());
}

#[test]
fn test_select_remove() {
    let (tx1, rx1) = mpmc::bounded_blocking::<i32>(10);
    let (tx2, rx2) = mpsc::bounded_blocking::<i32>(10);

    let mut select = Select::new();
    select.add(&rx1);
    select.add(&rx2);

    select.remove(&rx1);

    tx1.send(100).unwrap();

    // Should timeout because rx1 is removed
    let res = select.select_timeout(Duration::from_millis(100));
    assert!(res.is_err());

    tx2.send(200).unwrap();
    let res = select.select().unwrap();
    assert!(res == rx2);
    assert_eq!(rx2.read_select(res).unwrap(), 200);
}

#[test]
fn test_select_loop() {
    let (tx1, rx1) = mpmc::bounded_blocking::<i32>(10);
    let (tx2, rx2) = mpsc::bounded_blocking::<i32>(10);
    let (tx3, rx3) = spsc::bounded_blocking::<i32>(10);

    let mut select = Select::new();
    select.add(&rx1);
    select.add(&rx2);
    select.add(&rx3);

    let t1 = thread::spawn(move || {
        for i in 0..10 {
            tx1.send(i).unwrap();
            thread::sleep(Duration::from_millis(10));
        }
    });

    let t2 = thread::spawn(move || {
        for i in 0..10 {
            tx2.send(i + 100).unwrap();
            thread::sleep(Duration::from_millis(10));
        }
    });

    let t3 = thread::spawn(move || {
        for i in 0..10 {
            tx3.send(i + 200).unwrap();
            thread::sleep(Duration::from_millis(10));
        }
    });

    let mut sum = 0;
    let mut count = 0;

    // Receive 30 messages
    while count < 30 {
        let res = select.select().unwrap();
        let val;
        if res == rx1 {
            val = rx1.read_select(res).unwrap();
        } else if res == rx2 {
            val = rx2.read_select(res).unwrap();
        } else if res == rx3 {
            val = rx3.read_select(res).unwrap();
        } else {
            panic!("unknown token");
        }
        sum += val;
        count += 1;
    }

    t1.join().unwrap();
    t2.join().unwrap();
    t3.join().unwrap();

    // Sum 0..10 = 45
    // Sum 100..110 = 1000 + 45 = 1045
    // Sum 200..210 = 2000 + 45 = 2045
    // Total = 45 + 1045 + 2045 = 3135
    assert_eq!(sum, 3135);
}

#[test]
fn test_select_remove_mid() {
    // Test removing a receiver from the middle of the list
    let (tx1, rx1) = mpmc::bounded_blocking::<i32>(10);
    let (tx2, rx2) = mpsc::bounded_blocking::<i32>(10);
    let (tx3, rx3) = spsc::bounded_blocking::<i32>(10);

    let mut select = Select::new();
    select.add(&rx1);
    select.add(&rx2);
    select.add(&rx3);

    // Remove rx2 (middle)
    select.remove(&rx2);

    tx1.send(1).unwrap();
    tx3.send(3).unwrap();
    tx2.send(2).unwrap(); // Should be ignored

    let mut results = Vec::new();
    for _ in 0..2 {
        let res = select.select().unwrap();
        if res == rx1 {
            results.push(rx1.read_select(res).unwrap());
        } else if res == rx3 {
            results.push(rx3.read_select(res).unwrap());
        } else {
            panic!("Unexpected token");
        }
    }

    // Should not receive from rx2
    assert!(select.select_timeout(Duration::from_millis(50)).is_err());

    results.sort();
    assert_eq!(results, vec![1, 3]);
}
