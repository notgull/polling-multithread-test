// Apache 2.0 License

use std::io::{self, prelude::*};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;
use std::thread;

const NUM_PIPES: usize = 100;
const NUM_THREADS: usize = 4;

fn main() {
    // Channel to send notifications down.
    let (sender, receiver) = async_channel::unbounded();

    // Create NUM_PIPES TCP pairs.
    let mut readers = Vec::new();
    let mut writers = Vec::new();
    
    for _ in 0..NUM_PIPES {
        let (reader, writer) = tcp_pair().unwrap();
        readers.push(reader);
        writers.push(writer);
    }

    // Create a thread that randomly writes to a random writer.
    let writer_handle = thread::Builder::new().name("writer".to_string())
        .spawn(move || {
            let rng = fastrand::Rng::new();

            while !writers.is_empty() {
                // Remove a random writer.
                let index = rng.usize(..writers.len());
                let mut writer = writers.remove(index);

                // Write to it.
                let data = [1, 2, 3, 4, 5];
                writer.write_all(&data).unwrap();

                // Wait for a random amount of ms from 1 to 10
                let ms = rng.u64(1..10);
                thread::sleep(Duration::from_millis(ms));
            }

            println!("Writer thread: Done writing");
        }).unwrap();
        
    // Spawn NUM_THREADS threads that read from the readers.
    let readers = Arc::new(readers);
    let mut handles = Vec::with_capacity(NUM_THREADS);

    for i in 0..NUM_THREADS {
        let readers = readers.clone();
        let sender = sender.clone();
        let handle = thread::Builder::new().name(format!("reader-{i}")).spawn(move || {
            // Create our poller and register our streams.
            let poller = polling::Poller::new().unwrap();
            for (i, reader) in readers.iter().enumerate() {
                poller.add_with_mode(reader, polling::Event::readable(i), polling::PollMode::Edge).unwrap();
            }

            // Wait for a writer to write.
            let mut events = Vec::new();
            loop {
                poller.wait(&mut events, Some(Duration::from_secs(5))).unwrap();
                if events.is_empty() {
                    println!("Reader thread {i}: No events");
                    break;
                }

                // Read from the readers.
                for event in events.drain(..) {
                    sender.send_blocking(event.key).unwrap();

                    // Delete after delivery.
                    poller.delete(&readers[event.key]).unwrap();
                }
            }
        }).expect("failed to spawn thread");
        handles.push(handle);
    }

    // Join every thread.
    writer_handle.join().unwrap();
    for handle in handles {
        handle.join().unwrap();
    }

    // Figure out what kind of events we have.
    drop(sender);
    let mut total_events = futures_lite::stream::block_on(receiver).collect::<Vec<usize>>();
    total_events.sort_unstable();

    // Find duplicates.
    let mut duplicates = 0;
    let mut last_duplicate = usize::MAX;

    for (i, event) in total_events.iter().enumerate() {
        if i > 0 && event == &total_events[i - 1] && *event != last_duplicate {
            duplicates += 1;
            last_duplicate = *event;
        }
    }

    println!("Total events: {}", total_events.len());
    println!("Total duplicates: {duplicates}");
}

fn tcp_pair() -> io::Result<(TcpStream, TcpStream)> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let a = TcpStream::connect(listener.local_addr()?)?;
    let (b, _) = listener.accept()?;
    Ok((a, b))
}
