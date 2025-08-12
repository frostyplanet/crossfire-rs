use crate::tests::common::{async_join_result, async_spawn, runtime_block_on, sleep, timeout};
use crate::*;
use captains_log::{logfn, *};
use rstest::*;
use std::time::Duration;

#[fixture]
fn setup_log() {
    let _ = recipe::env_logger("LOG_FILE", "LOG_LEVEL").build().expect("log setup");
}

// Macro to wrap tests with a 5-second timeout
macro_rules! runtime_block_on_with_timeout {
    ($async_block:expr) => {{
        runtime_block_on!(async move {
            timeout(Duration::from_secs(5), $async_block)
                .await
                .expect("Test timed out after 5 seconds")
        })
    }};
}

// Test async-to-blocking receiver switching for bounded channels with messages in buffer
#[logfn]
#[rstest]
#[case(spsc::bounded_async::<usize>(5))] // Small buffer to create backpressure
#[case(mpsc::bounded_async::<usize>(5))]
fn test_bounded_async_with_sync_receiver_switch_buffered<T: AsyncTxTrait<usize>>(
    setup_log: (), #[case] channel: (T, AsyncRx<usize>),
) {
    let (tx, rx) = channel;
    let total_messages = 20; // More messages than buffer capacity
    let async_consumed = 4; // Leave messages in buffer during switch

    runtime_block_on_with_timeout!(async move {
        // Spawn async sender task - will block when buffer fills
        let sender_task = async_spawn!(async move {
            for i in 0..total_messages {
                debug!("Async sender sending message {}", i);
                tx.send(i).await.expect("Failed to send message");
            }
            debug!("Async sender completed all {} messages", total_messages);
        });

        // Consume some messages with async receiver (in async task)
        let receiver_task = async_spawn!(async move {
            let mut async_received = Vec::new();
            for _ in 0..async_consumed {
                match rx.recv().await {
                    Ok(value) => {
                        debug!("Async receiver got message: {}", value);
                        async_received.push(value);
                    }
                    Err(e) => {
                        panic!("Failed to receive message: {:?}", e);
                    }
                }
            }
            debug!("Async receiver consumed {} messages", async_received.len());
            (rx, async_received)
        });

        // Get the receiver back after partial consumption
        let (rx, async_received) = async_join_result!(receiver_task);

        // CRITICAL: Convert to blocking receiver while messages are still in buffer AND sender is waiting
        let blocking_rx: Rx<usize> = rx.into();

        // Continue receiving with blocking receiver in a thread
        let remaining_messages = total_messages - async_consumed;
        let sync_th = std::thread::spawn(move || {
            let mut sync_received = Vec::new();

            while let Ok(value) = blocking_rx.recv() {
                debug!("Sync receiver got message: {}", value);
                sync_received.push(value);
            }

            debug!("Sync receiver consumed {} messages from buffer", sync_received.len());
            sync_received
        });

        // Wait for sender to complete
        let _ = sender_task.await;

        let sync_received = sync_th.join().expect("Sync receiver thread panicked");

        // Verify all messages were received
        assert_eq!(async_received.len(), async_consumed);
        assert_eq!(sync_received.len(), remaining_messages);

        let mut all_received = async_received;
        all_received.extend(sync_received);
        assert_eq!(all_received.len(), total_messages);

        for i in 0..total_messages {
            assert!(all_received.contains(&i), "Missing value: {}", i);
        }

        debug!(
            "Successfully switched bounded channel from async to sync receiver with backpressure"
        );
    });
}

// Test async-to-blocking receiver switching for MPMC bounded channels with messages in buffer
#[logfn]
#[rstest]
#[case(mpmc::bounded_async::<usize>(5))] // Small buffer to create backpressure
fn test_mpmc_bounded_async_with_sync_receiver_switch_buffered(
    setup_log: (), #[case] channel: (MAsyncTx<usize>, MAsyncRx<usize>),
) {
    let (tx, rx) = channel;
    let total_messages = 20; // More messages than buffer capacity
    let async_consumed = 4; // Consume some messages before switching

    runtime_block_on_with_timeout!(async move {
        // Send all messages first to fill buffer (async sender in task)
        let sender_task = async_spawn!(async move {
            for i in 0..total_messages {
                tx.send(i).await.expect("Failed to send message");
                debug!("Async sender sent message: {}", i);
            }
            debug!("Async sender completed all {} messages", total_messages);
        });

        // Consume some messages with async receiver (in async task)
        let receiver_task = async_spawn!(async move {
            let mut async_received = Vec::new();
            for _ in 0..async_consumed {
                match rx.recv().await {
                    Ok(value) => {
                        debug!("Async receiver got message: {}", value);
                        async_received.push(value);
                    }
                    Err(e) => {
                        panic!("Failed to receive message with async receiver: {:?}", e);
                    }
                }
            }
            debug!("Async receiver consumed {} messages", async_received.len());
            (rx, async_received)
        });

        // Get the receiver back after partial consumption
        let (rx, async_received) = async_join_result!(receiver_task);

        // Convert to blocking receiver while messages are still in buffer
        let sync_rx: MRx<usize> = rx.into();

        // Consume remaining messages with blocking receiver in thread
        let remaining_messages = total_messages - async_consumed;
        let sync_th = std::thread::spawn(move || {
            let mut sync_received = Vec::new();
            while let Ok(value) = sync_rx.recv() {
                debug!("Sync receiver got message: {}", value);
                sync_received.push(value);
            }
            debug!("Sync receiver consumed {} messages from buffer", sync_received.len());
            sync_received
        });

        // Wait for sender to complete
        let _ = sender_task.await;
        let sync_received = sync_th.join().expect("Sync receiver thread panicked");

        // Verify all messages were received
        assert_eq!(async_received.len(), async_consumed);
        assert_eq!(sync_received.len(), remaining_messages);

        let mut all_received = async_received;
        all_received.extend(sync_received);
        assert_eq!(all_received.len(), total_messages);

        for i in 0..total_messages {
            assert!(all_received.contains(&i), "Missing value: {}", i);
        }

        debug!(
            "Successfully switched MPMC bounded channel from async to sync receiver with backpressure"
        );
    });
}

// Test blocking-to-async sender switching for bounded channels
#[logfn]
#[rstest]
#[case(spsc::bounded_blocking::<usize>(5))] // Small buffer for backpressure
fn test_spsc_bounded_blocking_with_async_sender_switch(
    setup_log: (), #[case] channel: (Tx<usize>, Rx<usize>),
) {
    let (tx, rx) = channel;
    let total_messages = 20;
    let sync_sent = 4; // Fill buffer, then switch while sender would block

    runtime_block_on_with_timeout!(async move {
        // Start blocking receiver in a thread
        let receiver_handle = std::thread::spawn(move || {
            let mut all_received = Vec::new();
            while let Ok(value) = rx.recv() {
                debug!("Blocking receiver got message: {}", value);
                all_received.push(value);
            }
            debug!("Blocking receiver completed");
            all_received
        });

        // Send messages with blocking sender in a thread (will block when buffer fills)
        let sender_handle = std::thread::spawn(move || {
            for i in 0..sync_sent {
                debug!("Blocking sender sending message {}", i);
                tx.send(i).expect("Failed to send message");
            }
            debug!("Blocking sender sent {} messages", sync_sent);
            tx
        });

        // Get the sender back and convert to async
        let tx = sender_handle.join().expect("Sender thread panicked");

        // CRITICAL: Convert to async sender while buffer has backpressure
        let async_tx: AsyncTx<usize> = tx.into();

        // Send remaining messages with async sender in task
        let remaining_messages = total_messages - sync_sent;
        let async_sender_task = async_spawn!(async move {
            for i in sync_sent..total_messages {
                debug!("Async sender sending message {}", i);
                async_tx.send(i).await.expect("Failed to send message");
            }
            debug!("Async sender sent {} more messages", remaining_messages);
        });
        // Wait for async sender to complete
        let _ = async_sender_task.await;
        // Get final results
        let all_received = receiver_handle.join().expect("Final receiver thread panicked");

        // Verify all messages were received
        assert_eq!(all_received.len(), total_messages);
        for i in 0..total_messages {
            assert!(all_received.contains(&i), "Missing value: {}", i);
        }

        debug!("Successfully switched from blocking to async sender with backpressure");
    });
}

// Test blocking-to-async sender switching for multi-producer bounded channels
#[logfn]
#[rstest]
#[case(mpsc::bounded_blocking::<usize>(5))] // Buffer < 12 total messages
fn test_mpsc_bounded_blocking_with_async_sender_switch(
    setup_log: (), #[case] channel: (MTx<usize>, Rx<usize>),
) {
    let (tx, rx) = channel;
    let total_messages = 20;
    let sync_sent = 4; // Fill buffer before switching

    runtime_block_on_with_timeout!(async move {
        // Send messages with blocking multi-sender in a thread
        let sender_handle = std::thread::spawn(move || {
            for i in 0..sync_sent {
                tx.send(i).expect("Failed to send message");
            }
            debug!("Blocking MTx sent {} messages, buffer has messages", sync_sent);
            tx
        });

        // Get the sender back
        let tx = sender_handle.join().expect("Sender thread panicked");

        // CRITICAL: Convert to async multi-sender while messages are in buffer
        let async_tx: MAsyncTx<usize> = tx.into();

        // Send remaining messages with async multi-sender in a task
        let async_sender_task = async_spawn!(async move {
            let remaining_messages = total_messages - sync_sent;
            for i in sync_sent..total_messages {
                async_tx.send(i).await.expect("Failed to send message");
            }
            debug!("Async MAsyncTx sent {} more messages", remaining_messages);
        });

        // Receive all messages with blocking receiver in a thread
        let receiver_handle = std::thread::spawn(move || {
            let mut all_received = Vec::new();
            while let Ok(value) = rx.recv() {
                all_received.push(value);
            }

            all_received
        });

        // Wait for async sender to complete
        let _ = async_sender_task.await;

        let all_received = receiver_handle.join().expect("Receiver thread panicked");

        // Verify all messages were received
        assert_eq!(all_received.len(), total_messages);
        for i in 0..total_messages {
            assert!(all_received.contains(&i), "Missing value: {}", i);
        }

        debug!("Successfully switched from blocking MTx to async MAsyncTx with buffered messages");
    });
}

// Test blocking-to-async sender switching for MPMC bounded channels
#[logfn]
#[rstest]
#[case(mpmc::bounded_blocking::<usize>(5))] // Buffer < 12 total messages
fn test_mpmc_bounded_blocking_with_async_sender_switch(
    setup_log: (), #[case] channel: (MTx<usize>, MRx<usize>),
) {
    let (tx, rx) = channel;
    let total_messages = 20;
    let sync_sent = 4; // Fill buffer before switching

    runtime_block_on_with_timeout!(async move {
        // Send messages with blocking multi-sender in a thread
        let sender_handle = std::thread::spawn(move || {
            for i in 0..sync_sent {
                tx.send(i).expect("Failed to send message");
            }
            debug!("Blocking MTx sent {} messages, buffer has messages", sync_sent);
            tx
        });

        // Get the sender back
        let tx = sender_handle.join().expect("Sender thread panicked");

        // CRITICAL: Convert to async multi-sender while messages are in buffer
        let async_tx: MAsyncTx<usize> = tx.into();

        // Send remaining messages with async multi-sender in a task
        let async_sender_task = async_spawn!(async move {
            let remaining_messages = total_messages - sync_sent;
            for i in sync_sent..total_messages {
                async_tx.send(i).await.expect("Failed to send message");
            }
            debug!("Async MAsyncTx sent {} more messages", remaining_messages);
        });

        // Receive all messages with blocking receiver in a thread
        let receiver_handle = std::thread::spawn(move || {
            let mut all_received = Vec::new();
            while let Ok(value) = rx.recv() {
                all_received.push(value);
            }

            all_received
        });

        // Wait for async sender to complete
        let _ = async_sender_task.await;
        let all_received = receiver_handle.join().expect("Receiver thread panicked");

        // Verify all messages were received
        assert_eq!(all_received.len(), total_messages);
        for i in 0..total_messages {
            assert!(all_received.contains(&i), "Missing value: {}", i);
        }

        debug!("Successfully switched from blocking MTx to async MAsyncTx with buffered messages for MPMC");
    });
}

// Test blocking-to-async receiver switching for bounded channels
#[logfn]
#[rstest]
#[case(spsc::bounded_blocking::<usize>(5))] // Buffer < 12 total messages
fn test_spsc_bounded_blocking_with_async_receiver_switch(
    setup_log: (), #[case] channel: (Tx<usize>, Rx<usize>),
) {
    let (tx, rx) = channel;
    let total_messages = 20;
    let sync_consumed = 4; // Leave most messages in buffer

    runtime_block_on_with_timeout!(async move {
        // Send all messages in a thread (sync sender)
        let sender_handle = std::thread::spawn(move || {
            for i in 0..total_messages {
                tx.send(i).expect("Failed to send message");
            }
            debug!("Sent {} messages to buffer", total_messages);
        });

        // Start receiver in a thread to consume some messages
        let receiver_handle = std::thread::spawn(move || {
            let mut sync_received = Vec::new();
            for _ in 0..sync_consumed {
                sync_received.push(rx.recv().expect("Failed to receive message"));
            }
            debug!(
                "Blocking receiver consumed {} messages, {} remain in buffer",
                sync_received.len(),
                total_messages - sync_consumed
            );
            (rx, sync_received)
        });

        // Join receiver first, then sender
        let (rx, sync_received) = receiver_handle.join().expect("Receiver thread panicked");

        // CRITICAL: Convert to async receiver while messages are still in buffer
        let async_rx: AsyncRx<usize> = rx.into();

        // Consume remaining messages with async receiver in a task
        let async_receiver_task = async_spawn!(async move {
            let mut async_received = Vec::new();

            while let Ok(value) = async_rx.recv().await {
                async_received.push(value);
            }

            debug!("Async receiver consumed {} messages from buffer", async_received.len());
            async_received
        });

        let async_received = async_join_result!(async_receiver_task);
        sender_handle.join().expect("Sender thread panicked");

        // Verify all messages were received
        let remaining_messages = total_messages - sync_consumed;
        assert_eq!(sync_received.len(), sync_consumed);
        assert_eq!(async_received.len(), remaining_messages);

        let mut all_received = sync_received;
        all_received.extend(async_received);
        assert_eq!(all_received.len(), total_messages);

        for i in 0..total_messages {
            assert!(all_received.contains(&i), "Missing value: {}", i);
        }

        debug!("Successfully switched from blocking to async receiver with buffered messages");
    });
}

// Test blocking-to-async receiver switching for multi-producer bounded channels
#[logfn]
#[rstest]
#[case(mpsc::bounded_blocking::<usize>(5))] // Buffer < 12 total messages
fn test_mpsc_bounded_blocking_with_async_receiver_switch(
    setup_log: (), #[case] channel: (MTx<usize>, Rx<usize>),
) {
    let (tx, rx) = channel;
    let total_messages = 20;
    let sync_consumed = 4; // Leave most messages in buffer

    runtime_block_on_with_timeout!(async move {
        // Start sender in a thread (sync sender)
        let sender_handle = std::thread::spawn(move || {
            for i in 0..total_messages {
                tx.send(i).expect("Failed to send message");
            }
            debug!("Sent {} messages to buffer", total_messages);
        });

        // Start receiver in a thread to consume some messages
        let receiver_handle = std::thread::spawn(move || {
            let mut sync_received = Vec::new();
            for _ in 0..sync_consumed {
                sync_received.push(rx.recv().expect("Failed to receive message"));
            }
            debug!(
                "Blocking receiver consumed {} messages, {} remain in buffer",
                sync_received.len(),
                total_messages - sync_consumed
            );
            (rx, sync_received)
        });

        // Join receiver first, then sender
        let (rx, sync_received) = receiver_handle.join().expect("Receiver thread panicked");

        // CRITICAL: Convert to async receiver while messages are still in buffer
        let async_rx: AsyncRx<usize> = rx.into();

        // Consume remaining messages with async receiver in a task
        let async_receiver_task = async_spawn!(async move {
            let mut async_received = Vec::new();

            while let Ok(value) = async_rx.recv().await {
                async_received.push(value);
            }

            debug!("Async receiver consumed {} messages from buffer", async_received.len());
            async_received
        });

        let async_received = async_join_result!(async_receiver_task);
        sender_handle.join().expect("Sender thread panicked");

        // Verify all messages were received
        let remaining_messages = total_messages - sync_consumed;
        assert_eq!(sync_received.len(), sync_consumed);
        assert_eq!(async_received.len(), remaining_messages);

        let mut all_received = sync_received;
        all_received.extend(async_received);
        assert_eq!(all_received.len(), total_messages);

        for i in 0..total_messages {
            assert!(all_received.contains(&i), "Missing value: {}", i);
        }

        debug!("Successfully switched from blocking to async receiver with buffered messages");
    });
}

// Test blocking-to-async receiver switching for MPMC bounded channels
#[logfn]
#[rstest]
#[case(mpmc::bounded_blocking::<usize>(5))] // Buffer < 12 total messages
fn test_mpmc_bounded_blocking_with_async_receiver_switch(
    setup_log: (), #[case] channel: (MTx<usize>, MRx<usize>),
) {
    let (tx, rx) = channel;
    let total_messages = 20;
    let sync_consumed = 4; // Leave most messages in buffer

    runtime_block_on_with_timeout!(async move {
        // Send all messages in a thread (sync sender)
        let sender_handle = std::thread::spawn(move || {
            for i in 0..total_messages {
                tx.send(i).expect("Failed to send message");
            }
            debug!("Sent {} messages to buffer", total_messages);
        });

        // Start receiver in a thread to consume some messages
        let receiver_handle = std::thread::spawn(move || {
            let mut sync_received = Vec::new();
            for _ in 0..sync_consumed {
                sync_received.push(rx.recv().expect("Failed to receive message"));
            }
            debug!(
                "Blocking receiver consumed {} messages, {} remain in buffer",
                sync_received.len(),
                total_messages - sync_consumed
            );
            (rx, sync_received)
        });

        // Join receiver first, then sender
        let (rx, sync_received) = receiver_handle.join().expect("Receiver thread panicked");

        // CRITICAL: Convert to async receiver while messages are still in buffer
        let async_rx: MAsyncRx<usize> = rx.into();

        // Consume remaining messages with async receiver in a task
        let async_receiver_task = async_spawn!(async move {
            let mut async_received = Vec::new();
            while let Ok(value) = async_rx.recv().await {
                async_received.push(value);
            }
            debug!("Async receiver consumed {} remaining messages", async_received.len());
            async_received
        });

        let async_received = async_join_result!(async_receiver_task);
        sender_handle.join().expect("Sender thread panicked");

        // Verify all messages were received
        let mut all_received = sync_received;
        all_received.extend(async_received);
        assert_eq!(all_received.len(), total_messages);

        for i in 0..total_messages {
            assert!(all_received.contains(&i), "Missing value: {}", i);
        }

        debug!("Successfully switched MPMC from blocking to async receiver with buffered messages");
    });
}

// Test multi-producer sender switching (MTx to MAsyncTx)
#[logfn]
#[rstest]
#[case(mpsc::bounded_blocking::<usize>(5))] // Buffer < 20 total messages
#[case(mpmc::bounded_blocking::<usize>(5))]
fn test_multi_producer_sender_switch<R: BlockingRxTrait<usize>>(
    setup_log: (), #[case] channel: (MTx<usize>, R),
) {
    let (tx, rx) = channel;
    let total_messages = 20;
    let sync_sent = 4; // Fill most of buffer before switching

    runtime_block_on_with_timeout!(async move {
        // Start receiver first to consume messages as they arrive
        let receiver_handle = std::thread::spawn(move || {
            let mut all_received = Vec::new();
            while let Ok(value) = rx.recv() {
                all_received.push(value);
            }
            all_received
        });

        // Send messages with blocking multi-sender in a thread
        let sender_handle = std::thread::spawn(move || {
            for i in 0..sync_sent {
                tx.send(i).expect("Failed to send message");
            }
            debug!("Blocking MTx sent {} messages, buffer has messages", sync_sent);
            tx
        });

        // Get the sender back and convert to async
        let tx = sender_handle.join().expect("Sender thread panicked");
        let async_tx: MAsyncTx<usize> = tx.into();

        // Send remaining messages with async multi-sender in a task
        let async_sender_task = async_spawn!(async move {
            for i in sync_sent..total_messages {
                async_tx.send(i).await.expect("Failed to send message");
            }
            debug!("Async MAsyncTx sent {} more messages", total_messages - sync_sent);
        });

        // Wait for async sender to complete, then join receiver
        let _ = async_sender_task.await;
        let all_received = receiver_handle.join().expect("Receiver thread panicked");

        // Verify all messages were received
        assert_eq!(all_received.len(), total_messages);
        for i in 0..total_messages {
            assert!(all_received.contains(&i), "Missing value: {}", i);
        }

        debug!("Successfully switched from MTx to MAsyncTx with buffered messages");
    });
}

// Test async-to-blocking sender switching for SPSC bounded channels
#[logfn]
#[rstest]
#[case(spsc::bounded_async::<usize>(5))] // Small buffer for backpressure
fn test_spsc_bounded_async_with_blocking_sender_switch(
    setup_log: (), #[case] channel: (AsyncTx<usize>, AsyncRx<usize>),
) {
    let (tx, rx) = channel;
    let total_messages = 10;
    let async_sent = 4; // Fill buffer before switching

    runtime_block_on_with_timeout!(async move {
        // Send messages with async sender in a task
        let sender_task = async_spawn!(async move {
            for i in 0..async_sent {
                tx.send(i).await.expect("Failed to send message");
            }
            debug!("Async sender sent {} messages, buffer has messages", async_sent);
            tx
        });

        // Get the sender back and convert to blocking
        let tx = async_join_result!(sender_task);
        let blocking_tx: Tx<usize> = tx.into();

        // Send remaining messages with blocking sender in thread
        let blocking_sender_handle = std::thread::spawn(move || {
            let remaining_messages = total_messages - async_sent;
            for i in async_sent..total_messages {
                blocking_tx.send(i).expect("Failed to send message");
            }
            debug!("Blocking sender sent {} more messages", remaining_messages);
        });

        // Receive all messages with async receiver in a task
        let receiver_task = async_spawn!(async move {
            let mut all_received = Vec::new();
            while let Ok(value) = rx.recv().await {
                all_received.push(value);
            }
            all_received
        });

        // Wait for both sender and receiver to complete
        let all_received = async_join_result!(receiver_task);
        blocking_sender_handle.join().expect("Blocking sender thread panicked");

        // Verify all messages were received
        assert_eq!(all_received.len(), total_messages);
        for i in 0..total_messages {
            assert!(all_received.contains(&i), "Missing value: {}", i);
        }

        debug!("Successfully switched SPSC from async to blocking sender with buffered messages");
    });
}

// Test async-to-blocking sender switching for MPSC bounded channels
#[logfn]
#[rstest]
#[case(mpsc::bounded_async::<usize>(5))] // Small buffer for backpressure
fn test_mpsc_bounded_async_with_blocking_sender_switch(
    setup_log: (), #[case] channel: (MAsyncTx<usize>, AsyncRx<usize>),
) {
    let (tx, rx) = channel;
    let total_messages = 10;
    let async_sent = 4; // Fill buffer before switching

    runtime_block_on_with_timeout!(async move {
        // Send messages with async multi-sender in a task
        let sender_task = async_spawn!(async move {
            for i in 0..async_sent {
                tx.send(i).await.expect("Failed to send message");
            }
            debug!("Async MAsyncTx sent {} messages, buffer has messages", async_sent);
            tx
        });

        // Get the sender back and convert to blocking
        let tx = async_join_result!(sender_task);
        let blocking_tx: MTx<usize> = tx.into();

        // Send remaining messages with blocking multi-sender in thread
        let blocking_sender_handle = std::thread::spawn(move || {
            let remaining_messages = total_messages - async_sent;
            for i in async_sent..total_messages {
                blocking_tx.send(i).expect("Failed to send message");
            }
            debug!("Blocking MTx sent {} more messages", remaining_messages);
        });

        // Receive all messages with async receiver in a task
        let receiver_task = async_spawn!(async move {
            let mut all_received = Vec::new();
            while let Ok(value) = rx.recv().await {
                all_received.push(value);
            }
            all_received
        });

        // Wait for both sender and receiver to complete
        let all_received = async_join_result!(receiver_task);
        blocking_sender_handle.join().expect("Blocking sender thread panicked");

        // Verify all messages were received
        assert_eq!(all_received.len(), total_messages);
        for i in 0..total_messages {
            assert!(all_received.contains(&i), "Missing value: {}", i);
        }

        debug!("Successfully switched MPSC from async to blocking sender with buffered messages");
    });
}

// Test async-to-blocking sender switching for MPMC bounded channels
#[logfn]
#[rstest]
#[case(mpmc::bounded_async::<usize>(5))] // Small buffer for backpressure
fn test_mpmc_bounded_async_with_blocking_sender_switch(
    setup_log: (), #[case] channel: (MAsyncTx<usize>, MAsyncRx<usize>),
) {
    let (tx, rx) = channel;
    let total_messages = 10;
    let async_sent = 4; // Fill buffer before switching

    runtime_block_on_with_timeout!(async move {
        // Send messages with async multi-sender in a task
        let sender_task = async_spawn!(async move {
            for i in 0..async_sent {
                tx.send(i).await.expect("Failed to send message");
            }
            debug!("Async MAsyncTx sent {} messages, buffer has messages", async_sent);
            tx
        });

        // Get the sender back and convert to blocking
        let tx = async_join_result!(sender_task);
        let blocking_tx: MTx<usize> = tx.into();

        // Send remaining messages with blocking multi-sender in thread
        let blocking_sender_handle = std::thread::spawn(move || {
            let remaining_messages = total_messages - async_sent;
            for i in async_sent..total_messages {
                blocking_tx.send(i).expect("Failed to send message");
            }
            debug!("Blocking MTx sent {} more messages", remaining_messages);
        });

        // Receive all messages with async multi-receiver in a task
        let receiver_task = async_spawn!(async move {
            let mut all_received = Vec::new();
            while let Ok(value) = rx.recv().await {
                all_received.push(value);
            }
            all_received
        });

        // Wait for both sender and receiver to complete
        let all_received = async_join_result!(receiver_task);
        blocking_sender_handle.join().expect("Blocking sender thread panicked");

        // Verify all messages were received
        assert_eq!(all_received.len(), total_messages);
        for i in 0..total_messages {
            assert!(all_received.contains(&i), "Missing value: {}", i);
        }

        debug!("Successfully switched MPMC from async to blocking sender with buffered messages");
    });
}

// Test to verify timeout functionality - this test should timeout and fail
#[logfn]
#[should_panic]
#[rstest]
fn test_timeout_functionality(setup_log: ()) {
    runtime_block_on_with_timeout!(async move {
        // This should timeout after 5 seconds
        sleep(Duration::from_secs(10)).await;
    });
}
