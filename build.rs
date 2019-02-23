use std::env;

fn main() {
    if env::var("CARGO_FEATURE_DEFAULT").is_ok() {
        println!("default!");
    }
    if env::var("CARGO_FEATURE_JEMALLOC").is_ok() {
        println!("jemalloc!");
    }
    if env::var("CARGO_FEATURE_JEMALLOCATOR").is_ok() {
        println!("jemallocator!");
    }
    if env::var("CARGO_FEATURE_TCMALLOC").is_ok() {
        println!("tcmalloc!");
    }
    panic!();
}
