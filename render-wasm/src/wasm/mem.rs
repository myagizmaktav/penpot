use std::alloc::{alloc, Layout};
use std::ptr;

#[allow(unused_imports)]
use crate::error::{Error, Result};
use crate::mem::{BUFFERU8, LAYOUT_ALIGN};
use macros::wasm_error;

#[no_mangle]
#[wasm_error]
pub extern "C" fn alloc_bytes(len: usize) -> Result<*mut u8> {
    // A buffer still allocated here was abandoned by a previous hand-off that
    // never reached its consumer (see `discard_stale_buffer`). Dropping it keeps
    // the module alive instead of aborting it with "Bytes already allocated".
    crate::mem::discard_stale_buffer("alloc_bytes");

    unsafe {
        let layout = Layout::from_size_align_unchecked(len, LAYOUT_ALIGN);
        let ptr = alloc(layout);
        if ptr.is_null() {
            return Err(Error::CriticalError("Allocation failed".to_string()));
        }
        // TODO: Maybe this could be removed.
        ptr::write_bytes(ptr, 0, len);
        BUFFERU8 = Some(Vec::from_raw_parts(ptr, len, len));
        Ok(ptr)
    }
}

#[no_mangle]
#[wasm_error]
pub extern "C" fn free_bytes() -> Result<()> {
    crate::mem::free_bytes()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mem;
    use std::sync::Mutex;

    // `BUFFERU8` is a process-wide static and cargo runs tests in parallel.
    static BUFFER_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn alloc_bytes_recovers_from_an_abandoned_buffer() {
        let _guard = BUFFER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        mem::free_bytes().unwrap();

        // A host that throws between `alloc_bytes` and the consumer call
        // leaves the slot occupied...
        let first = alloc_bytes(8);
        assert!(!first.is_null());

        // ...the next allocation must not abort the module.
        let second = alloc_bytes(16);
        assert!(!second.is_null());

        let bytes = mem::bytes();
        assert_eq!(bytes.len(), 16);
        assert!(bytes.iter().all(|b| *b == 0));
        assert_eq!(mem::discard_stale_buffer("test"), None);
    }

    #[test]
    fn write_bytes_replaces_an_unread_result_buffer() {
        let _guard = BUFFER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        mem::free_bytes().unwrap();

        // A result buffer that JS never read/freed...
        mem::write_bytes(vec![1, 2, 3]);
        // ...must not make the next result panic with "Bytes already allocated".
        mem::write_bytes(vec![4, 5]);
        assert_eq!(mem::bytes(), vec![4, 5]);

        // The same applies when JS allocates after an unread result.
        mem::write_bytes(vec![7; 4]);
        assert!(!alloc_bytes(4).is_null());
        assert_eq!(mem::bytes(), vec![0; 4]);
    }

    #[test]
    fn discard_stale_buffer_reports_the_dropped_size() {
        let _guard = BUFFER_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        mem::free_bytes().unwrap();

        assert_eq!(mem::discard_stale_buffer("test"), None);
        mem::write_bytes(vec![0; 12]);
        assert_eq!(mem::discard_stale_buffer("test"), Some(12));
        assert_eq!(mem::discard_stale_buffer("test"), None);
    }
}
