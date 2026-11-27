use crate::{error::Result, performance};

pub const LAYOUT_ALIGN: usize = 4;

// Please, read about the #[allow(static_mut_refs)]
//
// If we don't put this allow, the compiler shows a warning like this:
//
// shared references to mutable statics are dangerous; it's undefined behavior
// if the static is mutated or if a mutable reference is created for it while
// the shared reference lives
//
// https://doc.rust-lang.org/edition-guide/rust-2024/static-mut-references.html
//
// But this isn't a problem in a single-threaded environment like WebAssembly
// because access/modification is always sequential, not parallel.
pub static mut BUFFERU8: Option<Vec<u8>> = None;
pub static mut BUFFER_ERROR: u8 = 0x00;

pub fn clear_error_code() {
    unsafe {
        BUFFER_ERROR = 0x00;
    }
}

/// Sets the error buffer from a byte. Used by #[wasm_error] when E: Into<u8>.
pub fn set_error_code(code: u8) {
    unsafe {
        BUFFER_ERROR = code;
    }
}

#[no_mangle]
pub extern "C" fn read_error_code() -> u8 {
    unsafe { BUFFER_ERROR }
}

/// Drops a transfer buffer that was allocated but never consumed.
///
/// `BUFFERU8` is a single-slot hand-off area between JS and WASM: JS allocates
/// it, writes into it and immediately calls the consumer, which takes it; or
/// WASM fills it and JS reads it and calls `free_bytes`. If the host throws
/// between those two steps (for example writing through a heap view that was
/// detached by a memory growth) the slot stays occupied. Because WASM runs
/// single-threaded and every hand-off is synchronous, an occupied slot at the
/// start of a new hand-off can only be an abandoned one, so it is safe to drop
/// it. Treating it as a fatal error used to abort the whole module (the panic
/// happens inside an `extern "C"` function, which cannot unwind).
///
/// Returns the size of the discarded buffer, if any.
pub fn discard_stale_buffer(context: &str) -> Option<usize> {
    unsafe {
        #[allow(static_mut_refs)]
        let stale = BUFFERU8.take();
        stale.map(|buffer| {
            println!(
                "Warning: {}: discarding an unconsumed transfer buffer of {} bytes",
                context,
                buffer.len()
            );
            buffer.len()
        })
    }
}

pub fn write_bytes(mut bytes: Vec<u8>) -> *mut u8 {
    unsafe {
        performance::begin_measure!("write_bytes");
        discard_stale_buffer("write_bytes");

        let ptr = bytes.as_mut_ptr();
        BUFFERU8 = Some(bytes);
        performance::end_measure!("write_bytes");
        ptr
    }
}

pub fn bytes() -> Vec<u8> {
    unsafe {
        #[allow(static_mut_refs)]
        BUFFERU8.take().expect("Buffer is not initialized")
    }
}

pub fn bytes_or_empty() -> Vec<u8> {
    unsafe {
        #[allow(static_mut_refs)]
        BUFFERU8.take().unwrap_or_default()
    }
}

pub fn free_bytes() -> Result<()> {
    unsafe {
        BUFFERU8 = None;
    }
    Ok(())
}

pub trait SerializableResult: From<Self::BytesType> + Into<Self::BytesType> {
    type BytesType;
    fn clone_to_slice(&self, slice: &mut [u8]);
}

/*
  Returns an array in the heap. The first 4 bytes is always the size
  of the array. Then the items are serialized one after the other
  by the implementation of SerializableResult trait
*/
pub fn write_vec<T: SerializableResult>(result: Vec<T>) -> *mut u8 {
    let elem_size = size_of::<T::BytesType>();
    let bytes_len = 4 + result.len() * elem_size;
    let mut result_bytes = vec![0; bytes_len];

    result_bytes[0..4].clone_from_slice(&result.len().to_le_bytes());

    for (i, item) in result.iter().enumerate() {
        let base = 4 + i * elem_size;
        item.clone_to_slice(&mut result_bytes[base..base + elem_size]);
    }

    write_bytes(result_bytes)
}

/*
  Like `write_vec`, but prepends an extra u32 header word before the
  length. Layout: [header u32][length u32][items...]
*/
pub fn write_vec_with_header<T: SerializableResult>(header: u32, result: Vec<T>) -> *mut u8 {
    let elem_size = size_of::<T::BytesType>();
    let bytes_len = 8 + result.len() * elem_size;
    let mut result_bytes = vec![0; bytes_len];

    result_bytes[0..4].clone_from_slice(&header.to_le_bytes());
    result_bytes[4..8].clone_from_slice(&result.len().to_le_bytes());

    for (i, item) in result.iter().enumerate() {
        let base = 8 + i * elem_size;
        item.clone_to_slice(&mut result_bytes[base..base + elem_size]);
    }

    write_bytes(result_bytes)
}
