use crate::*;
use captains_log::logfn;
use crossfire::select::{Multiplex, Mux, Select, SelectMode};
use crossfire::*;
use rstest::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

#[fixture]
fn setup_log() {
    _setup_log();
}

#[logfn]
#[rstest]
fn test_select_basic(setup_log: ()) {
    let (tx1, rx1) = mpmc::bounded_blocking::<i32>(10);
    let (tx2, rx2) = mpsc::bounded_blocking::<i32>(10);

    tx1.send(100).expect("send");
    tx2.send(200).expect("send");
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

#[logfn]
#[rstest]
fn test_select_basic_timeout(setup_log: ()) {
    let (_tx1, rx1) = mpmc::bounded_blocking::<i32>(10);
    let (_tx2, rx2) = mpmc::bounded_blocking::<i32>(10);
    let (_tx3, rx3) = mpmc::bounded_blocking::<i32>(10);
    let mut select = Select::new();
    select.add(&rx1);
    select.add(&rx2);
    select.add(&rx3);
    let start = std::time::Instant::now();
    let res = select.select_timeout(Duration::from_millis(100));
    let elapsed = start.elapsed();
    assert!(res.is_err());
    assert!(elapsed >= Duration::from_millis(100));
}

#[logfn]
#[rstest]
fn test_select_basic_disconnect_before_park(setup_log: ()) {
    let (_tx1, rx1) = mpmc::bounded_blocking::<i32>(10);
    let (_tx2, rx2) = mpmc::bounded_blocking::<i32>(10);
    let (_tx3, rx3) = mpmc::bounded_blocking::<i32>(10);
    let (_tx4, rx4) = mpmc::bounded_blocking::<i32>(10);
    let mut select = Select::new();
    select.add(&rx1);
    select.add(&rx2);
    select.add(&rx3);
    select.add(&rx4);
    drop(_tx3);
    let res = select.select();
    assert!(res.is_ok());
    let res = res.unwrap();
    assert!(res == rx3);
    // Disconnected and empty
    assert!(rx3.read_select(res).is_err());
    select.remove(&rx3);
    assert_eq!(select.try_select().unwrap_err(), TryRecvError::Empty);
    _tx2.send(200).expect("send");
    let res = select.select().unwrap();
    assert!(res == rx2);
    println!("select_result {:?}, rx2 {:?}", res, rx2);
    assert_eq!(rx2.read_select(res).unwrap(), 200);
}

#[logfn]
#[rstest]
fn test_select_basic_disconnect_after_park(setup_log: ()) {
    let (_tx1, rx1) = mpmc::bounded_blocking::<i32>(10);
    let (_tx2, rx2) = mpmc::bounded_blocking::<i32>(10);
    let (_tx3, rx3) = mpmc::bounded_blocking::<i32>(10);
    let (_tx4, rx4) = mpmc::bounded_blocking::<i32>(10);
    let mut select = Select::new();
    select.add(&rx1);
    select.add(&rx2);
    select.add(&rx3);
    select.add(&rx4);
    let barrier = Arc::new(Barrier::new(2));
    let _barrier = barrier.clone();
    let th = thread::spawn(move || {
        _barrier.wait();
        thread::sleep(Duration::from_millis(500));
        drop(_tx3);
    });
    barrier.wait();
    let res = select.select();
    assert!(res.is_ok());
    let res = res.unwrap();
    assert!(res == rx3);
    // Disconnected and empty
    assert!(rx3.read_select(res).is_err());
    let _ = th.join();
    select.remove(&rx3);
    assert_eq!(select.try_select().unwrap_err(), TryRecvError::Empty);
    _tx2.send(200).expect("send");
    let res = select.select().unwrap();
    assert!(res == rx2);
    assert_eq!(rx2.read_select(res).unwrap(), 200);
}

#[logfn]
#[rstest]
fn test_select_basic_loop(setup_log: ()) {
    let (tx1, rx1) = mpmc::unbounded_blocking::<i32>();
    let (tx2, rx2) = mpmc::bounded_blocking::<i32>(10);
    let (tx3, rx3): (MTx<mpmc::One<i32>>, MRx<mpmc::One<i32>>) = mpmc::build(mpmc::One::new());
    let (tx4, rx4) = mpsc::unbounded_blocking::<i32>();
    let (tx5, rx5) = mpsc::bounded_blocking::<i32>(10);
    let (tx6, rx6): (MTx<mpsc::One<i32>>, Rx<mpsc::One<i32>>) = mpsc::new();

    let mut select = Select::new();
    select.add(&rx1);
    select.add(&rx2);
    select.add(&rx3);
    select.add(&rx4);
    select.add(&rx5);
    select.add(&rx6);

    let t1 = thread::spawn(move || {
        for i in 0..10 {
            tx1.send(i).expect("send");
            thread::sleep(Duration::from_millis(10));
        }
    });

    let t2 = thread::spawn(move || {
        for i in 0..10 {
            tx2.send(i + 100).expect("send");
            thread::sleep(Duration::from_millis(10));
        }
    });

    let t3 = thread::spawn(move || {
        for i in 0..10 {
            tx3.send(i + 200).expect("send");
            thread::sleep(Duration::from_millis(10));
        }
    });

    let t4 = thread::spawn(move || {
        for i in 0..10 {
            tx4.send(i + 300).expect("send");
            thread::sleep(Duration::from_millis(10));
        }
    });

    let t5 = thread::spawn(move || {
        for i in 0..10 {
            tx5.send(i + 400).expect("send");
            thread::sleep(Duration::from_millis(10));
        }
    });

    let t6 = thread::spawn(move || {
        for i in 0..10 {
            tx6.send(i + 500).expect("send");
            thread::sleep(Duration::from_millis(10));
        }
    });

    let mut sum = 0;
    loop {
        let res = match select.select() {
            Ok(res) => res,
            Err(RecvError) => {
                println!("All channels disconnected or removed from select. Breaking loop.");
                break;
            }
        };

        if res == rx1 {
            match rx1.read_select(res) {
                Ok(val) => {
                    sum += val;
                }
                Err(RecvError) => {
                    println!("rx1 disconnected, removing from select.");
                    select.remove(&rx1);
                }
            }
        } else if res == rx2 {
            match rx2.read_select(res) {
                Ok(val) => {
                    sum += val;
                }
                Err(RecvError) => {
                    println!("rx2 disconnected, removing from select.");
                    select.remove(&rx2);
                }
            }
        } else if res == rx3 {
            match rx3.read_select(res) {
                Ok(val) => {
                    sum += val;
                }
                Err(RecvError) => {
                    println!("rx3 disconnected, removing from select.");
                    select.remove(&rx3);
                }
            }
        } else if res == rx4 {
            match rx4.read_select(res) {
                Ok(val) => {
                    sum += val;
                }
                Err(RecvError) => {
                    println!("rx4 disconnected, removing from select.");
                    select.remove(&rx4);
                }
            }
        } else if res == rx5 {
            match rx5.read_select(res) {
                Ok(val) => {
                    sum += val;
                }
                Err(RecvError) => {
                    println!("rx5 disconnected, removing from select.");
                    select.remove(&rx5);
                }
            }
        } else if res == rx6 {
            match rx6.read_select(res) {
                Ok(val) => {
                    sum += val;
                }
                Err(RecvError) => {
                    println!("rx6 disconnected, removing from select.");
                    select.remove(&rx6);
                }
            }
        } else {
            panic!("unknown token");
        }
    }

    t1.join().unwrap();
    t2.join().unwrap();
    t3.join().unwrap();
    t4.join().unwrap();
    t5.join().unwrap();
    t6.join().unwrap();

    assert_eq!(sum, 15270);
}

#[logfn]
#[rstest]
fn test_select_remove_mid(setup_log: ()) {
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

#[logfn]
#[rstest]
fn test_select_mixed_flavors(setup_log: ()) {
    // Test mixing List (unbounded), Array (bounded > 1) and One (explicit One)
    let (tx_list, rx_list) = mpmc::unbounded_blocking::<i32>();
    let (tx_array, rx_array) = mpmc::bounded_blocking::<i32>(10);
    let (tx_one, rx_one): (MTx<mpmc::One<i32>>, MRx<mpmc::One<i32>>) =
        mpmc::build(mpmc::One::new());

    let mut select = Select::new();
    select.add(&rx_list);
    select.add(&rx_array);
    select.add(&rx_one);

    tx_list.send(1).expect("send");
    tx_array.send(2).expect("send");
    tx_one.send(3).expect("send");

    let mut results = Vec::new();
    for _ in 0..3 {
        let res = select.select().unwrap();
        if res == rx_list {
            results.push(rx_list.read_select(res).unwrap());
        } else if res == rx_array {
            results.push(rx_array.read_select(res).unwrap());
        } else if res == rx_one {
            results.push(rx_one.read_select(res).unwrap());
        } else {
            panic!("Unexpected token");
        }
    }

    results.sort();
    assert_eq!(results, vec![1, 2, 3]);
}

#[logfn]
#[rstest]
#[case(1)]
#[case(5)]
fn test_select_pressure(setup_log: (), #[case] producers: usize) {
    let (tx_list, rx_list) = mpmc::unbounded_blocking::<usize>();
    let (tx_array, rx_array) = mpmc::bounded_blocking::<usize>(100);
    let (tx_one, rx_one): (MTx<mpmc::One<usize>>, MRx<mpmc::One<usize>>) =
        mpmc::build(mpmc::One::new());
    let (tx_mpsc_list, rx_mpsc_list) = mpsc::unbounded_blocking::<usize>();
    let (tx_mpsc_array, rx_mpsc_array) = mpsc::bounded_blocking::<usize>(100);
    let (tx_mpsc_one, rx_mpsc_one): (MTx<mpsc::One<usize>>, Rx<mpsc::One<usize>>) = mpsc::new();

    let mut select = Select::new();
    select.add(&rx_list);
    select.add(&rx_array);
    select.add(&rx_one);
    select.add(&rx_mpsc_list);
    select.add(&rx_mpsc_array);
    select.add(&rx_mpsc_one);

    let round = ROUND;
    let total_messages = round * 6 * producers;
    let mut handlers = Vec::new();

    for _ in 0..producers {
        let tx = tx_list.clone();
        handlers.push(thread::spawn(move || {
            for i in 0..round {
                tx.send(i).expect("send");
            }
        }));

        let tx = tx_array.clone();
        handlers.push(thread::spawn(move || {
            for i in 0..round {
                tx.send(i).expect("send");
            }
        }));

        let tx = tx_one.clone();
        handlers.push(thread::spawn(move || {
            for i in 0..round {
                tx.send(i).expect("send");
            }
        }));

        let tx = tx_mpsc_list.clone();
        handlers.push(thread::spawn(move || {
            for i in 0..round {
                tx.send(i).expect("send");
            }
        }));

        let tx = tx_mpsc_array.clone();
        handlers.push(thread::spawn(move || {
            for i in 0..round {
                tx.send(i).expect("send");
            }
        }));

        let tx = tx_mpsc_one.clone();
        handlers.push(thread::spawn(move || {
            for i in 0..round {
                tx.send(i).expect("send");
            }
        }));
    }

    // Drop original senders to ensure we don't hang if we were counting on close
    drop(tx_list);
    drop(tx_array);
    drop(tx_one);
    drop(tx_mpsc_list);
    drop(tx_mpsc_array);
    drop(tx_mpsc_one);

    let mut count = 0;
    while count < total_messages {
        let res = select.select();
        match res {
            Ok(token) => {
                if token == rx_list {
                    if rx_list.read_select(token).is_ok() {
                        count += 1;
                    }
                } else if token == rx_array {
                    if rx_array.read_select(token).is_ok() {
                        count += 1;
                    }
                } else if token == rx_one {
                    if rx_one.read_select(token).is_ok() {
                        count += 1;
                    }
                } else if token == rx_mpsc_list {
                    if rx_mpsc_list.read_select(token).is_ok() {
                        count += 1;
                    }
                } else if token == rx_mpsc_array {
                    if rx_mpsc_array.read_select(token).is_ok() {
                        count += 1;
                    }
                } else if token == rx_mpsc_one {
                    if rx_mpsc_one.read_select(token).is_ok() {
                        count += 1;
                    }
                } else {
                    panic!("unknown token");
                }
            }
            Err(_) => {
                break;
            }
        }
    }

    for h in handlers {
        h.join().unwrap();
    }

    assert_eq!(count, total_messages);
}

#[logfn]
#[rstest]
fn test_select_pressure_concurrent(setup_log: ()) {
    let (tx_list, rx_list) = mpmc::unbounded_blocking::<i32>();
    let (tx_array, rx_array) = mpmc::bounded_blocking::<i32>(100);
    let mut th_recv = Vec::new();
    for _ in 0..2 {
        let rx_list_clone = rx_list.clone();
        let rx_array_clone = rx_array.clone();
        th_recv.push(thread::spawn(move || {
            let mut select = Select::new();
            select.add(&rx_list_clone);
            select.add(&rx_array_clone);
            let mut local_sum: usize = 0;
            loop {
                match select.select() {
                    Ok(res) => {
                        if res == rx_list_clone {
                            if rx_list_clone.read_select(res).is_err() {
                                select.remove(&rx_list_clone);
                            } else {
                                local_sum += 1;
                            }
                        } else if res == rx_array_clone {
                            if rx_array_clone.read_select(res).is_err() {
                                select.remove(&rx_array_clone);
                            } else {
                                local_sum += 1;
                            }
                        } else {
                            unreachable!();
                        }
                    }
                    Err(_) => break,
                }
            }
            local_sum
        }));
    }
    let mut th_send = Vec::new();
    for _ in 0..2 {
        let tx_list_clone = tx_list.clone();
        let tx_array_clone = tx_array.clone();
        th_send.push(thread::spawn(move || {
            for i in 0..ROUND {
                tx_list_clone.send(i as i32).expect("send");
            }
        }));
        th_send.push(thread::spawn(move || {
            for i in 0..ROUND {
                tx_array_clone.send((i + ROUND) as i32).expect("send");
            }
        }));
    }
    drop(tx_list);
    drop(tx_array);
    for th in th_send {
        let _ = th.join();
    }
    let mut total_sum = 0;
    for th in th_recv {
        total_sum += th.join().unwrap();
    }
    assert_eq!(total_sum, 4 * ROUND);
}

#[logfn]
#[rstest]
fn test_multiplex_basic(setup_log: ()) {
    let mut mp = Multiplex::<mpsc::Array<i32>>::new();
    let tx1: MTx<_> = mp.bounded_tx(10);
    let tx2: MTx<_> = mp.bounded_tx(10);

    // Send values from different threads
    let h1 = thread::spawn(move || {
        tx1.send(1).unwrap();
    });
    let h2 = thread::spawn(move || {
        tx2.send(2).unwrap();
    });

    // Collect received values
    let mut received = Vec::new();
    for _ in 0..2 {
        let val = mp.recv().unwrap();
        received.push(val);
    }

    h1.join().unwrap();
    h2.join().unwrap();

    // Verify we received both values (order may vary due to round-robin selection)
    assert!(received.contains(&1));
    assert!(received.contains(&2));
    assert_eq!(received.len(), 2);
}

#[logfn]
#[rstest]
fn test_multiplex_modes(setup_log: ()) {
    let mut mp_rr = Multiplex::<mpsc::Array<i32>>::new_with(SelectMode::RR);
    let tx: MTx<_> = mp_rr.bounded_tx(10);
    tx.send(42).unwrap();
    assert_eq!(mp_rr.recv().unwrap(), 42);

    let mut mp_rand = Multiplex::<mpmc::Array<i32>>::new_random();
    let tx: MTx<_> = mp_rand.bounded_tx(10);
    tx.send(100).unwrap();
    assert_eq!(mp_rand.recv().unwrap(), 100);

    let mut mp_bias = Multiplex::<spsc::Array<i32>>::new_bias();
    let tx: Tx<_> = mp_bias.bounded_tx(10);
    tx.send(200).unwrap();
    assert_eq!(mp_bias.recv().unwrap(), 200);
}

#[logfn]
#[rstest]
fn test_multiplex_timeout(setup_log: ()) {
    let mut mp = Multiplex::<mpmc::Array<i32>>::new();
    let _tx: MTx<_> = mp.bounded_tx(10);
    let result = mp.recv_timeout(Duration::from_millis(10));
    assert_eq!(result, Err(RecvTimeoutError::Timeout));
}

#[logfn]
#[rstest]
fn test_multiplex_try_recv(setup_log: ()) {
    let mut mp = Multiplex::<mpmc::Array<i32>>::new();
    let tx: MTx<_> = mp.bounded_tx(10);
    assert_eq!(mp.try_recv(), Err(TryRecvError::Empty));
    tx.send(42).unwrap();
    assert_eq!(mp.try_recv(), Ok(42));
    assert_eq!(mp.try_recv(), Err(TryRecvError::Empty));
}

#[logfn]
#[rstest]
fn test_multiplex_basic_array_blocking(setup_log: ()) {
    let mut mp = Multiplex::<mpsc::Array<i32>>::new();
    let tx1: MTx<_> = mp.bounded_tx(10);
    let tx2: MTx<_> = mp.bounded_tx(10);
    let tx3: MTx<_> = mp.bounded_tx(10);

    let h1 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(50));
        tx1.send(10).expect("send");
    });
    let h2 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(100));
        tx2.send(20).expect("send");
    });
    let h3 = thread::spawn(move || {
        thread::sleep(Duration::from_millis(25));
        tx3.send(30).expect("send");
    });
    let mut received_values = Vec::new();
    for _ in 0..3 {
        received_values.push(mp.recv().unwrap());
    }
    received_values.sort();
    assert_eq!(received_values, vec![10, 20, 30]);
    h1.join().unwrap();
    h2.join().unwrap();
    h3.join().unwrap();
}

#[logfn]
#[rstest]
fn test_multiplex_basic_list_blocking(setup_log: ()) {
    let mut mp = Multiplex::<mpsc::List<i32>>::new();
    let tx1: MTx<Mux<mpsc::List<i32>>> = mp.new_tx();
    let tx2: MTx<Mux<mpsc::List<i32>>> = mp.new_tx();

    let h1 = thread::spawn(move || {
        tx1.send(10).expect("send");
    });
    let h2 = thread::spawn(move || {
        tx2.send(20).expect("send");
    });
    let mut received_values = Vec::new();
    for _ in 0..2 {
        received_values.push(mp.recv().unwrap());
    }
    received_values.sort();
    assert_eq!(received_values, vec![10, 20]);

    h1.join().unwrap();
    h2.join().unwrap();
}

#[logfn]
#[rstest]
fn test_multiplex_sender_close(setup_log: ()) {
    let mut mp = Multiplex::<mpsc::Array<i32>>::new();
    let tx1: MTx<_> = mp.bounded_tx(10);
    let tx2: MTx<_> = mp.bounded_tx(10);

    tx1.send(1).expect("send");
    tx2.send(2).expect("send");

    drop(tx1);
    drop(tx2);

    let mut received = 0;
    while let Ok(_) = mp.recv() {
        received += 1;
    }
    assert_eq!(received, 2);
}

#[logfn]
#[rstest]
#[case(1, 1)]
#[case(5, 1)]
#[case(5, 5)]
fn test_multiplex_basic_drop_on_sender_blocked(
    setup_log: (), #[case] producers: usize, #[case] bound: usize,
) {
    macro_rules! run_test {
        ($flavor: path, $tx_t: tt)=>{{
            let mut mp = Multiplex::<$flavor>::new();
            println!("run_test {:?}", mp);
            let mut senders: Vec<$tx_t<Mux<$flavor>>> = Vec::new();
            for _ in 0..producers {
                senders.push(mp.bounded_tx(bound));
            }
            let results = Arc::new(AtomicUsize::new(0)); // To count how many senders returned disconnected
                                                         // Fill the channel initially so the first sender blocks
            for tx in &senders {
                for i in 0..bound {
                    tx.send(i).expect("send"); // Fill up the capacity
                }
            }
            let mut handles = Vec::new();
            let barrier = Arc::new(Barrier::new(producers + 1)); // +1 for the main thread
            for tx in senders {
                let barrier_clone = barrier.clone();
                let results_clone = results.clone();
                handles.push(thread::spawn(move || {
                    barrier_clone.wait(); // Wait for all senders to be ready to block
                    let res = tx.send(100);
                    if let Err(SendError(_)) = res {
                        results_clone.fetch_add(1, Ordering::SeqCst);
                    }
                }));
            }
            barrier.wait(); // Main thread waits for all sender threads to reach the barrier
                            // Give a moment for threads to potentially block
            thread::sleep(Duration::from_millis(50));
            // Drop the multiplexer, which should wake up all blocking senders
            drop(mp);
            for handle in handles {
                handle.join().unwrap();
            }
            assert_eq!(results.load(Ordering::SeqCst), producers);
            println!("");
        }};
    }
    run_test!(spsc::Array<usize>, Tx);
    run_test!(mpsc::Array<usize>, MTx);
    run_test!(mpmc::Array<usize>, MTx);
}

#[logfn]
#[rstest]
#[case(1, 1)]
#[case(1, 10)]
#[case(20, 1)]
#[case(10, 10)]
#[case(5, 100)]
fn test_pressure_multiplex_array(setup_log: (), #[case] producers: usize, #[case] bound: usize) {
    let mut mp = Multiplex::<spsc::Array<usize>>::new();
    let round = ROUND;
    let total_messages = round * producers;
    let mut handlers = Vec::new();

    for _ in 0..producers {
        let tx: Tx<_> = mp.bounded_tx(bound);
        handlers.push(thread::spawn(move || {
            for i in 0..round {
                tx.send(i).expect("send");
            }
        }));
    }

    let mut count = 0;
    while count < total_messages {
        match mp.recv() {
            Ok(_) => count += 1,
            Err(_) => break,
        }
    }
    for h in handlers {
        h.join().unwrap();
    }
    assert_eq!(count, total_messages);
}

#[logfn]
#[rstest]
#[case(1, 1)]
#[case(1, 10)]
#[case(20, 1)]
#[case(10, 10)]
#[case(5, 20)]
fn test_pressure_multiplex_array_mp(setup_log: (), #[case] producers: usize, #[case] bound: usize) {
    macro_rules! run_test {
        ($mp: expr) => {
            println!("run_test {:?}", $mp);
            let round = ROUND;
            let total_messages = round * producers * 4;
            let mut handlers = Vec::new();
            for _ in 0..producers {
                let tx: MTx<_> = $mp.bounded_tx(bound);
                for _ in 0..4 {
                    let _tx = tx.clone();
                    handlers.push(thread::spawn(move || {
                        for i in 0..round {
                            _tx.send(i).expect("send");
                        }
                    }));
                }
            }
            let mut count = 0;
            while count < total_messages {
                match $mp.recv() {
                    Ok(_) => count += 1,
                    Err(_) => break,
                }
            }
            for h in handlers {
                h.join().unwrap();
            }
            assert_eq!(count, total_messages);
        };
    }
    let mut mp = Multiplex::<mpsc::Array<usize>>::new();
    run_test!(mp);
    let mut mp = Multiplex::<mpmc::Array<usize>>::new();
    run_test!(mp);
}

#[logfn]
#[rstest]
#[case(1)]
#[case(5)]
#[case(20)]
fn test_pressure_multiplex_list(setup_log: (), #[case] producers: usize) {
    macro_rules! run_test {
        ($mp: expr, $tx_c: tt) => {
            println!("run_test {:?}", $mp);
            let round = ROUND;
            let total_messages = round * producers;
            let mut handlers = Vec::new();
            for _ in 0..producers {
                let tx: $tx_c<_> = $mp.new_tx();
                handlers.push(thread::spawn(move || {
                    for i in 0..round {
                        tx.send(i).expect("send");
                    }
                }));
            }

            let mut count = 0;
            while count < total_messages {
                match $mp.recv() {
                    Ok(_) => count += 1,
                    Err(_) => break,
                }
            }

            for h in handlers {
                h.join().unwrap();
            }
            assert_eq!(count, total_messages);
        };
    }
    let mut mp = Multiplex::<spsc::List<usize>>::new();
    run_test!(mp, Tx);
    let mut mp = Multiplex::<mpsc::List<usize>>::new();
    run_test!(mp, MTx);
    let mut mp = Multiplex::<mpmc::List<usize>>::new();
    run_test!(mp, MTx);
}
